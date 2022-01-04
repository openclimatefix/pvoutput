"""Retrieve Natural Earth world boundaries."""
import logging
import os
from io import BytesIO
from typing import List, Tuple

import geopandas as gpd
import requests
from numpy.typing import NDArray


class NaturalEarth:
    """Retrieve Natural Earth world boundaries."""

    def __init__(self, cache_dir: str = None) -> None:
        """Initialise.

        Args:
            cache_dir:
                Optionally provide a location to cache boundary definition files locally and avoid
                unnecsessary downloads.

        Raises:
            ValueError: If the cache_dir does not exist.
        """
        self.cache_dir = cache_dir
        if self.cache_dir is not None:
            if not os.path.isdir(cache_dir):
                logging.error("The cache_dir does not exist.")
                raise ValueError("The cache_dir does not exist.")
        self.world_hires = None
        self.countries_hires = None
        self.world_lores = None
        self.countries_lores = None

    def get_hires_world_boundaries(self) -> Tuple[gpd.GeoDataFrame, NDArray]:
        """Load high res world boundaries.

        Download the high resolution country boundaries GIS file from the Natural Earth website and
        optionally cache locally.

        Returns:
            A tuple containing (`world`, `countries`). `world` is a Geopandas GeoDataFrame with
            geometries and metadata for all country borders. `countries` is a list of unique country
            names for which geometries exist. Boundaries will be in the EPSG:4087 projected CRS.

        Typical usage example:
            world, countries = get_world_boundaries()
        """
        if self.world_hires is not None and self.countries_hires is not None:
            return self.world_hires, self.countries_hires
        if self.cache_dir:
            cache_file = os.path.join(self.cache_dir, "ne_10m_admin_0_countries.zip")
        else:
            cache_file = None
        if cache_file is not None and os.path.isfile(cache_file):
            data = cache_file
        else:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36"
            }
            url = (
                "https://www.naturalearthdata.com/http//www.naturalearthdata.com/"
                "download/10m/cultural/ne_10m_admin_0_countries.zip"
            )
            req = requests.get(url, headers=headers)
            data = BytesIO(req.content)
            if cache_file is not None:
                with open(cache_file, "wb") as fid:
                    fid.write(req.content)
        self.world_hires = gpd.read_file(data).to_crs("EPSG:4087")
        cols2keep = {"NAME": "name", "CONTINENT": "continent", "geometry": "geometry"}
        self.world_hires = self.world_hires[list(cols2keep.keys())].rename(columns=cols2keep)
        self.countries_hires = self.world_hires.name.unique()
        return self.world_hires, self.countries_hires

    def get_lores_world_boundaries(self) -> Tuple[gpd.GeoDataFrame, NDArray]:
        """Load low resolution world boundaries.

        Load the low res world boundaries GIS file (`naturalearth_lowres`) from Geopandas datasets.
        Useful for visualisations and/or to speed up computation.

        Returns:
            A tuple containing (`world`, `countries`). `world` is a Geopandas GeoDataFrame with
            geometries and metadata for all country borders. `countries` is a list of unique country
            names for which geometries exist. Boundaries will be in the EPSG:4087 projected CRS.

        Typical usage example:
            world, countries = get_world_boundaries()
        """
        if self.world_lores is not None and self.countries_lores is not None:
            return self.world_lores, self.countries_lores
        self.world_lores = gpd.read_file(gpd.datasets.get_path("naturalearth_lowres")).to_crs(
            "EPSG:4087"
        )
        self.world_lores.drop(columns=["pop_est", "iso_a3", "gdp_md_est"], inplace=True)
        self.countries = self.world_lores.name.unique()
        return self.world_lores, self.countries_lores

    def list_countries(self, res: str = "hires") -> Tuple[gpd.GeoDataFrame, NDArray]:
        """Print a list of country names to stdout.

        Print a list of country names whose geometries are available in the world boundaries GIS
        file.

        Args:
            res:
                Optionally switch between 'hires' and 'lores', although in theory both country lists
                should be identical, there may be some countries that are not included in the lores
                boundaries due to downsampling of borders. Names may also have changed between the
                two (e.g. Macedonia -> North Macedonia).

        Raises:
            ValueError: If `res` is not one of: 'lores', 'hires'.
        """
        if res == "hires":
            _, countries = self.get_hires_world_boundaries()
        elif res == "lores":
            _, countries = self.get_lores_world_boundaries()
        else:
            logging.error("The `res` arg should be one of: 'lores', 'hires'.")
            raise ValueError("The `res` arg should be one of: 'lores', 'hires'.")
        countries.sort()
        print(f"Available countries are:\n{', '.join(countries)}")

        return countries
