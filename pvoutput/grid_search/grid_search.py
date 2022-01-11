"""
Grid Search Class used to make grid of latitude and longitude coordinates
"""

from typing import Iterable, Optional

import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from pyproj import Transformer

from pvoutput.grid_search.clip import (
    bounding_box_from_radius,
    buffer_bounding_box_bounds,
    clip_to_bbox,
    clip_to_countries,
    clip_to_radius,
)
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
        # get countries
        world, all_countries = self.nat_earth.get_hires_world_boundaries()
        countries = all_countries if countries is None else countries

        # create bounding box
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

        # create x and y bounds
        search_radius_m = search_radius * 1000.0
        wgs84_to_projected = Transformer.from_crs(4326, local_crs_epsg, always_xy=True)
        projected_to_wgs84 = Transformer.from_crs(local_crs_epsg, 4326, always_xy=True)
        xmin, ymin = wgs84_to_projected.transform(bounds[1], bounds[0])
        xmax, ymax = wgs84_to_projected.transform(bounds[3], bounds[2])
        y_interval = search_radius_m * np.cos(np.radians(30))
        x_interval = search_radius_m * 3
        x_offset = 0

        # create coordinates
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
        coords = clip_to_countries(
            coords=coords,
            world=world,
            countries=countries,
            buffer=buffer,
            search_radius=search_radius,
        )
        coords = clip_to_bbox(
            coords=coords,
            bbox=bbox,
            buffer=buffer,
            search_radius=search_radius,
            local_crs_epsg=local_crs_epsg,
        )
        if radial_clip is not None:
            coords = clip_to_radius(
                coords=coords,
                latitude=radial_clip[0],
                longitude=radial_clip[1],
                radius=radial_clip[2],
                search_radius=search_radius,
                local_crs_epsg=local_crs_epsg,
            )

        # show plot
        if show:
            self.plot_grid(
                coords,
                countries,
                bbox,
            )

        # save plots
        if save_to is not None:
            coords.to_csv(
                save_to, float_format="%.5f", index=False, columns=["longitude", "latitude"]
            )
        return coords[["latitude", "longitude"]].reset_index(drop=True)
