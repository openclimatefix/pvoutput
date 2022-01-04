"""Clipping function for coordinates"""
from typing import Iterable, Optional, Tuple, Union

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point, Polygon


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
