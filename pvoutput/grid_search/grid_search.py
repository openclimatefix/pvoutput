#!/usr/bin/env python3

"""
Generate gridded lat/lon coordinates that can be used for fixed radius searches across a region.

Provides both an importable method and a CLI.

.. code:: console

    $ python grid_search.py -h

- Jamie Taylor <jamie.taylor@sheffield.ac.uk>
- First Authored: 2021-11-16
"""

import argparse
import logging
import os
import sys
from typing import Iterable, Optional, Tuple, Union

import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from pyproj import Transformer
from shapely.geometry import Point, Polygon

from pvoutput.grid_search.natural_earth import NaturalEarth


class GridSearch:
    """A class for generating a gridded search."""

    def __init__(self, cache_dir: str = None) -> None:
        """Initialise.

        Args:
            cache_dir:
                Optionally provide a location to cache boundary definition files locally and avoid
                unnecsessary downloads.
        """
        self.nat_earth = NaturalEarth(cache_dir)

    def plot_grid(
        self,
        coords: gpd.GeoDataFrame,
        countries: Iterable[str],
        bbox: Optional[Iterable[float]] = None,
        local_crs_epsg: int = 4087,
        filename: Optional[str] = None,
    ) -> None:
        """Plot grid coordinates.

        Plot grid coordinates over world boundaries with selected countries highlighted.

        Args:
            coords:
                A geopandas GeoDataFrame containing latitudes, longitudes and geometries for a set
                of coordinates.
            countries:
                A list of country names to clip the coords to.
            bbox:
                Optionally pass a four element iterable defining a bounding box:
                [min_lat, min_lon, max_lat, max_lon]. This will be used to set the scale of the
                plot.
            local_crs_epsg:
                Optionally provide the EPSG code of a local Co-ordinate Reference System (CRS) for
                improved accuracy. e.g. set to 27700 (OSGB36 / British National Grid) if searching
                the British Isles. The default is EPSG:4087 (a.k.a WGS 84 / World Equidistant
                Cylindrical), which works globally but with less accuracy.
            filename:
                Optionally pass a filename (relative or absolute) to save the plot to. Image format
                should be set using the file extension (i.e. .jpeg, .png or .svg).
        """
        world, _ = self.nat_earth.get_hires_world_boundaries()
        world.to_crs(f"EPSG:{local_crs_epsg}", inplace=True)
        coords.to_crs(f"EPSG:{local_crs_epsg}", inplace=True)
        if bbox is None:
            bbox = [
                coords.latitude.min(),
                coords.longitude.min(),
                coords.latitude.max(),
                coords.longitude.max(),
            ]
        f, ax = plt.subplots()
        world.plot(
            ax=ax, color="palegreen", edgecolor="black", linewidth=1, label="World", zorder=1
        )
        if countries is not None:
            selected = world[world.name.isin(countries)]
            selected.geometry.boundary.plot(
                ax=ax, color=None, edgecolor="gold", label="Selected countries", zorder=2
            )
        coords.plot(ax=ax, marker="o", color="red", markersize=2, label="Grid", zorder=3)
        xmin = coords.geometry.bounds.minx.min()
        xmax = coords.geometry.bounds.maxx.max()
        ymin = coords.geometry.bounds.miny.min()
        ymax = coords.geometry.bounds.maxy.max()
        xpadding = (xmax - xmin) / 8
        ypadding = (ymax - ymin) / 8
        ax.set_xlim(xmin - xpadding, xmax + xpadding)
        ax.set_ylim(ymin - ypadding, ymax + ypadding)
        ax.axes.xaxis.set_visible(False)
        ax.axes.yaxis.set_visible(False)
        plt.legend(prop={"size": 6})
        plt.show()
        if filename is not None:
            plt.savefig(filename, dpi=300)

    def generate_grid(
        self,
        bbox: Optional[Iterable[float]] = None,
        countries: Optional[Iterable[str]] = None,
        radial_clip: Optional[Iterable[float]] = None,
        buffer: float = 0,
        search_radius: float = 25,
        local_crs_epsg: int = 4087,
        save_to: Optional[str] = None,
        show: bool = False,
    ):
        """Use hexagonal tiling to generate a grid search with minimal overlap.

        Create a set of gridded coordinates which use hexagonal tiling as an efficient way to
        conduct a fixed-radius search of a region defined by a bounding box and/or country borders.

        Args:
            bbox:
                Optionally pass a four element iterable defining a bounding box:
                [min_lat, min_lon, max_lat, max_lon].
            countries:
                Optionally pass a list of country names to clip the coords to.
            radial_clip:
                Optionally set a radial boundary to clip to. Pass a three element iterable
                containing: (<lat>, <lon>, <radius_km>).
            buffer:
                Optionally buffer the bounding box and country boundaries before clipping, in
                kilometers.
            search_radius:
                Optionally set the radial search limit around each grid point in kilometers.
                Defaults to 25km.
            local_crs_epsg:
                Optionally provide the EPSG code of a local co-ordinate Reference System (CRS) for
                improved accuracy. e.g. set to 27700 (OSGB36 / British National Grid) if searching
                the British Isles. The default is EPSG:4087 (a.k.a WGS 84 / World Equidistant
                Cylindrical), which works globally but with poor accuracy in some locations.
            save_to:
                Optionally specify a filename to save the grid co-ordinates to (CSV).
            show:
                Set to True to show a plot of the grid.

        Returns:
            A pandas DataFrame containing co-ordinates for the grid with columns: latitude,
            longitude.
        """
        world, all_countries = self.nat_earth.get_hires_world_boundaries()
        countries = all_countries if countries is None else countries
        if bbox is None:
            if radial_clip is None:
                bbox = (
                    world[world.name.isin(countries)]
                    .dissolve()
                    .buffer(buffer * 1000.0)
                    .to_crs("EPSG:4326")
                    .bounds.to_numpy()[0]
                )
                bbox = [bbox[1], bbox[0], bbox[3], bbox[2]]
            else:
                bbox = bounding_box_from_radius(
                    radial_clip[0], radial_clip[1], radial_clip[2], local_crs_epsg
                )
        bbox = buffer_bounding_box_bounds(bbox, buffer)
        bounds = [np.round(b, 5) for b in bbox]
        search_radius_m = search_radius * 1000.0
        wgs84_to_projected = Transformer.from_crs(4326, local_crs_epsg, always_xy=True)
        projected_to_wgs84 = Transformer.from_crs(local_crs_epsg, 4326, always_xy=True)
        xmin, ymin = wgs84_to_projected.transform(bounds[1], bounds[0])
        xmax, ymax = wgs84_to_projected.transform(bounds[3], bounds[2])
        y_interval = search_radius_m * np.cos(np.radians(30))
        x_interval = search_radius_m * 3
        x_offset = 0
        coords = []
        for y in np.arange(ymin - y_interval * 3, ymax + y_interval + search_radius_m, y_interval):
            xmin_ = xmin - search_radius_m - x_offset
            xmax_ = xmax + x_interval + search_radius_m + x_offset
            for x in np.arange(xmin_, xmax_, x_interval):
                coords.append(projected_to_wgs84.transform(x, y))
            if x_offset == 0:
                x_offset = search_radius_m * 1.5
            else:
                x_offset = 0
        coords = pd.DataFrame(coords, columns=["longitude", "latitude"])
        coords = gpd.GeoDataFrame(
            coords, geometry=gpd.points_from_xy(coords.longitude, coords.latitude)
        )
        coords = coords.set_crs("EPSG:4326").to_crs(f"EPSG:4087")
        coords = clip_to_countries(coords, world, countries, buffer, search_radius)
        coords = clip_to_bbox(coords, bbox, buffer, search_radius, local_crs_epsg)
        if radial_clip is not None:
            coords = clip_to_radius(
                coords,
                radial_clip[0],
                radial_clip[1],
                radial_clip[2],
                search_radius,
                local_crs_epsg,
            )
        if show:
            self.plot_grid(
                coords,
                countries,
                bbox,
            )
        if save_to is not None:
            coords.to_csv(
                save_to, float_format="%.5f", index=False, columns=["longitude", "latitude"]
            )
        return coords[["latitude", "longitude"]].reset_index(drop=True)


def clip_to_countries(
    coords: gpd.GeoDataFrame,
    world: gpd.GeoDataFrame,
    countries: Iterable[str],
    buffer: float = 0,
    search_radius: Optional[float] = None,
) -> gpd.GeoDataFrame:
    """Clip coordinates to country boundaries.

    Given a set of coordinates, some country boundary definitions, a list of countries, and a buffer
    distance, return the coords which fall within `buffer` kilometers of the listed countries'
    boundaries.

    Args:
        coords:
            A geopandas GeoDataFrame containing latitudes, longitudes and geometries for a set of
            coordinates.
        world:
            A geopandas GeoDataFrame of world boundaries geomteries, as returned by
            `get_world_boundaries()`.
        countries:
            A list of country names to clip the coords to.
        buffer:
            Optionally buffer the country boundaries before clipping, in kilometers.
        search_radius:
            Optionally set the radial search limit around each grid point in kilometers. If set, the
            code will consider coords to be included if any part of the search radius overlaps the
            country.

    Returns:
        As per `coords` but containing only the subset of the input coordinates which fall within
        `buffer` km of the given countries.
    """
    countries_ = world[world.name.isin(countries)]
    countries_ = countries_.dissolve().buffer(buffer * 1000.0)[0]
    if search_radius is None:
        coords["selected"] = coords.within(countries_)
    else:
        # Consider points outside the selected region whose search radius overlaps the region
        coords_ = coords.buffer(search_radius * 1000.0)
        coords["selected"] = coords_.intersects(countries_)
    return coords.loc[coords["selected"]].drop(columns="selected")


def clip_to_bbox(
    coords: Union[pd.DataFrame, gpd.GeoDataFrame],
    bbox: Iterable[float],
    buffer: float = 0,
    search_radius: Optional[float] = None,
    local_crs_epsg: int = 4087,
) -> Union[pd.DataFrame, gpd.GeoDataFrame]:
    """Clip coordinates to bounding box.

    Remove any coordinates which do not lie inside a bounding box.

    Args:
        coords:
            A pandas DataFrame or geopandas GeoDataFrame of coordinates with columns: latitude,
            longitude.
        bbox:
            Four element iterable defining a bounding box: [min_lat, min_lon, max_lat, max_lon].
        buffer:
            Optionally buffer the country boundaries before clipping, in kilometers.
        search_radius:
            Optionally set the radial search limit around each grid point in kilometers. If set, the
            code will consider coords to be included if any part of the search radius overlaps the
            bounding box.
        local_crs_epsg:
            Optionally provide the EPSG code of a local Co-ordinate Reference System (CRS) for
            improved accuracy. e.g. set to 27700 (OSGB36 / British National Grid) if searching the
            British Isles.

    Returns:
        As per `coords` but containing only the subset of the input coordinates which fall within
        `buffer` km of the bounding box.
    """
    if search_radius is None:
        bounds = buffer_bounding_box_bounds(bbox, buffer, local_crs_epsg)
        coords["selected"] = (bbox[0] <= coords.latitude <= bbox[2]) & (
            bbox[1] <= coords.longitude <= bbox[3]
        )
    else:
        bbox_ = buffer_bounding_box(bbox, buffer, local_crs_epsg)[0]
        coords_ = coords.to_crs(f"EPSG:{local_crs_epsg}").buffer(search_radius * 1000.0)
        coords["selected"] = coords_.intersects(bbox_)
    return coords.loc[coords["selected"]].drop(columns="selected")


def buffer_bounding_box(
    bbox: Optional[Iterable[float]] = None, buffer: float = 0, local_crs_epsg: int = 4087
) -> gpd.GeoSeries:
    """Buffer a bounding box by a distance in km.

    Args:
        bbox:
            Four element iterable defining a bounding box: [min_lat, min_lon, max_lat, max_lon].
        buffer:
            Optionally buffer the country boundaries before clipping, in kilometers.
        local_crs_epsg:
            Optionally provide the EPSG code of a local Co-ordinate Reference System (CRS) for
            improved accuracy. e.g. set to 27700 (OSGB36 / British National Grid) if searching the
            British Isles.

    Returns:
        A geopandas GeoSeries containing the buffered geometry.
    """
    bbox_ = Polygon(
        [(bbox[1], bbox[0]), (bbox[1], bbox[2]), (bbox[3], bbox[2]), (bbox[3], bbox[0])]
    )
    bbox_ = (
        gpd.GeoSeries(bbox_)
        .set_crs("EPSG:4326")
        .to_crs(f"EPSG:{local_crs_epsg}")
        .buffer(buffer * 1000.0)
    )
    return bbox_


def buffer_bounding_box_bounds(
    bbox: Optional[Iterable[float]] = None, buffer: float = 0, local_crs_epsg: int = 4087
) -> Tuple[float]:
    """Buffer a bounding box by a distance in km.

    Args:
        bbox:
            Four element iterable defining a bounding box: [min_lat, min_lon, max_lat, max_lon].
        buffer:
            Optionally buffer the country boundaries before clipping, in kilometers.
        local_crs_epsg:
            Optionally provide the EPSG code of a local Co-ordinate Reference System (CRS) for
            improved accuracy. e.g. set to 27700 (OSGB36 / British National Grid) if searching the
            British Isles.

    Returns:
        Four element tuple defining a bounding box: (min_lat, min_lon, max_lat, max_lon).
    """
    bbox_ = buffer_bounding_box(bbox, buffer, local_crs_epsg)
    new_bounds = bbox_.to_crs("EPSG:4326").bounds.loc[0].to_numpy()
    return new_bounds[1], new_bounds[0], new_bounds[3], new_bounds[2]


def bounding_box_from_radius(
    latitude: float, longitude: float, radius: float, local_crs_epsg: int = 4087
) -> Tuple[float]:
    """Convert a radial search around a given lat/lon to a bounding box.

    Args:
        latitude:
            Latitude of the center of the radial search.
        longitude:
            Longitude of the center of the radial search.
        radius:
            Set the radial search limit in km.
        local_crs_epsg:
            Optionally provide the EPSG code of a local Co-ordinate Reference System (CRS) for
            improved accuracy. e.g. set to 27700 (OSGB36 / British National Grid) if searching the
            British Isles.

    Returns:
        Four element tuple defining a bounding box: (min_lat, min_lon, max_lat, max_lon).
    """
    center = Point(longitude, latitude)
    search_radius = (
        gpd.GeoSeries(center)
        .set_crs("EPSG:4326")
        .to_crs(f"EPSG:{local_crs_epsg}")
        .buffer(radius * 1000.0)
    )
    bounds = search_radius.to_crs("EPSG:4326").bounds.loc[0].to_numpy()
    return bounds[1], bounds[0], bounds[3], bounds[2]


def clip_to_radius(
    coords: Union[pd.DataFrame, gpd.GeoDataFrame],
    latitude: float,
    longitude: float,
    radius: Optional[float] = None,
    search_radius: Optional[float] = None,
    local_crs_epsg: int = 4087,
) -> Union[pd.DataFrame, gpd.GeoDataFrame]:
    """Clip coordinates to a radius.

    Remove any coordinates which do not lie within x.

    Args:
        coords:
            A pandas DataFrame or geopandas GeoDataFrame of coordinates with columns: latitude,
            longitude.
        latitude:
            Latitude of the center of the radial search.
        longitude:
            Longitude of the center of the radial search.
        radius:
            Set the radial search limit in km.
        search_radius:
            Optionally set the radial search limit around each grid point in kilometers. If set, the
            code will consider coords to be included if any part of the search radius overlaps the
            outter radius.
        local_crs_epsg:
            Optionally provide the EPSG code of a local Co-ordinate Reference System (CRS) for
            improved accuracy. e.g. set to 27700 (OSGB36 / British National Grid) if searching the
            British Isles.

    Returns:
        As per `coords` but containing only the subset of the input coordinates which fall within
        `radius` km of the lat/lon.
    """
    center = Point(longitude, latitude)
    radius_ = (
        gpd.GeoSeries(center)
        .set_crs("EPSG:4326")
        .to_crs(f"EPSG:{local_crs_epsg}")
        .buffer(radius * 1000.0)[0]
    )
    if search_radius is None:
        coords["selected"] = coords.within(radius_)
    else:
        coords_ = coords.to_crs(f"EPSG:{local_crs_epsg}").buffer(search_radius * 1000.0)
        coords["selected"] = coords_.intersects(radius_)
    return coords.loc[coords["selected"]].drop(columns="selected")


def query_yes_no(question: str, default: Optional[str] = "yes") -> bool:
    """Ask a yes/no question via input() and return the answer as boolean.

    Args:
        question:
            The question presented to the user.
        default:
            The presumed answer if the user just hits <Enter>. It must be "yes" (the default), "no"
            or None (meaning an answer is required of the user).

    Returns:
        Return value is True for "yes" or False for "no".
    """
    valid = {"yes": True, "y": True, "ye": True, "no": False, "n": False}
    if default is None:
        prompt = " [y/n] "
    elif default == "yes":
        prompt = " [Y/n] "
    elif default == "no":
        prompt = " [y/N] "
    else:
        raise ValueError("invalid default answer: '%s'" % default)
    while True:
        sys.stdout.write(question + prompt)
        choice = input().lower()
        if default is not None and choice == "":
            return valid[default]
        elif choice in valid:
            return valid[choice]
        else:
            sys.stdout.write("Please respond with 'yes' or 'no' " "(or 'y' or 'n').\n")


def parse_options():
    """Parse command line options."""
    parser = argparse.ArgumentParser(
        description=("This is a command line interface (CLI) for " "the grid_search module."),
        epilog="Jamie Taylor, 2021-11-16",
    )
    parser.add_argument(
        "--bbox",
        metavar="<min_lat,min_lon,max_lat,max_lon>",
        dest="bbox",
        action="store",
        type=str,
        required=False,
        default=None,
        help="Specify a bounding box to search. Can be used in conjunction with " "--countries",
    )
    parser.add_argument(
        "--countries",
        metavar="<country>[,<country>[,...]]",
        dest="countries",
        action="store",
        type=str,
        required=False,
        default=None,
        help="Specify a list of countries, searching only grid points that fall "
        "within these countries' boundaries. Specify one or more countries, "
        "separated by commas (default is all). Country names must match those "
        "used in the Natural Earth dataset (HINT: run this code with the "
        "--list-countries option to list them). This option can be used in "
        "conjunction with --bbox, in which case the search will only include "
        "grid points within both the bounding box and the countries list.",
    )
    parser.add_argument(
        "--radial-clip",
        metavar="<lat,lon,radius_km>",
        dest="radial_clip",
        action="store",
        type=str,
        required=False,
        default=None,
        help="Specify a radius to clip to. Can be used in conjunction with --bbox "
        "and --countries. Pass the latitude, longitude and radius as a "
        "comma-separated string. Radius should be in km.",
    )
    parser.add_argument(
        "--list-countries",
        dest="list_countries",
        action="store_true",
        required=False,
        help="List the country names that can be used for the " "--countries option.",
    )
    parser.add_argument(
        "--buffer",
        metavar="<buffer>",
        dest="buffer",
        action="store",
        type=float,
        required=False,
        default=0.0,
        help="Specify a buffer/tolerance for including grid points i.e. include "
        "grid points that fall within <buffer> kilometers of the target "
        "boundary. Default is 0km.",
    )
    parser.add_argument(
        "--search-radius",
        metavar="<radius>",
        dest="search_radius",
        action="store",
        type=float,
        required=False,
        default=25.0,
        help="Specify the radial search limit around each grid point in " "kilometers.",
    )
    parser.add_argument(
        "--local-crs-epsg",
        metavar="<EPSG>",
        dest="local_crs_epsg",
        action="store",
        type=int,
        required=False,
        default=4087,
        help="Optionally provide the EPSG code of a local co-ordinate Reference "
        "System (CRS) for improved accuracy. e.g. set to 27700 (OSGB36 / "
        "British National Grid) if searching the British Isles.",
    )
    parser.add_argument(
        "--cache-dir",
        metavar="</path/to/dir>",
        dest="cache_dir",
        action="store",
        type=str,
        required=False,
        default=None,
        help="Specify a directory to use for caching downloaded boundary files.",
    )
    parser.add_argument(
        "--show",
        dest="show",
        action="store_true",
        required=False,
        help="Set this flag to show a plot of the grid.",
    )
    parser.add_argument(
        "-o",
        "--outfile",
        metavar="<path>",
        dest="outfile",
        action="store",
        type=str,
        required=False,
        help="Specify a filename to save the grid to.",
    )
    options = parser.parse_args()

    def handle_options(options):
        """Validate command line args and pre-process where necessary."""
        if options.bbox is not None:
            options.bbox = list(map(lambda x: float(x.strip()), options.bbox.split(",")))
        if options.radial_clip is not None:
            options.radial_clip = list(
                map(lambda x: float(x.strip()), options.radial_clip.split(","))
            )
        if options.cache_dir is not None:
            if not os.path.isdir(options.cache_dir):
                logging.error(f"The cache_dir '{options.cache_dir}' does not exist.")
                raise ValueError(f"The cache_dir '{options.cache_dir}' does not exist.")
        if options.countries:
            options.countries = list(map(lambda x: str(x.strip()), options.countries.split(",")))
            earth = NaturalEarth(options.cache_dir)
            _, countries = earth.get_hires_world_boundaries()
            for country in options.countries:
                if country not in countries:
                    logging.error(f"The country '{country}' is invalid.")
                    raise ValueError(f"The country '{country}' is invalid.")
        if options.outfile is not None and os.path.exists(options.outfile):
            check = query_yes_no(
                f"The output file '{options.outfile}' already exists, results "
                "will be overwritten, do you want to continue?",
                "no",
            )
            if check is False:
                print("Quitting...")
                sys.exit(0)
        return options

    return handle_options(options)


def main():
    options = parse_options()
    grd = GridSearch(cache_dir=options.cache_dir)
    if options.list_countries:
        grd.nat_earth.list_countries()
        sys.exit()
    grd.generate_grid(
        bbox=options.bbox,
        countries=options.countries,
        radial_clip=options.radial_clip,
        buffer=options.buffer,
        search_radius=options.search_radius,
        local_crs_epsg=options.local_crs_epsg,
        save_to=options.outfile,
        show=options.show,
    )


if __name__ == "__main__":
    fmt = "%(asctime)s [%(levelname)s] - %(message)s (%(filename)s:%(funcName)s)"
    datefmt = "%Y-%m-%dT%H:%M:%SZ"
    logging.basicConfig(format=fmt, datefmt=datefmt, level=os.environ.get("LOGLEVEL", "INFO"))
    main()
