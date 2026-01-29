import inspect
import os
import pytest
from bs4 import BeautifulSoup
import geopandas as gpd
from shapely.geometry import Polygon


# --- Helper to find the data directory ---
@pytest.fixture
def data_dir():
    # Taken from http://stackoverflow.com/a/6098238/732596
    data_dir = os.path.dirname(inspect.getfile(inspect.currentframe()))
    data_dir = os.path.abspath(data_dir)
    return data_dir

# --- LOADS THE HTML FILE (For Mapscraper Tests) ---
@pytest.fixture
def map_soup():
    fpath = os.path.join(os.path.dirname(__file__), 'data', 'map_search.html')
    
    if not os.path.exists(fpath):
        pytest.fail(f"Missing test data: {fpath}")
    
    with open(fpath, 'r', encoding='utf-8') as f:
        return BeautifulSoup(f.read(), 'html.parser')

# --- MOCKS GEOGRAPHIC DATA (For Grid Search Tests) ---
@pytest.fixture
def mock_natural_earth(mocker):
    """
    Mocks the Natural Earth data download.
    Returns a fake world map (EPSG:4087) covering Europe to ensure tests pass.
    """
    # 1. Create a massive polygon covering all of Europe
    # Longitude: -20 (Atlantic) to 50 (Russia)
    # Latitude: 30 (Africa) to 80 (Arctic)
    # Defined in Lat/Lon first (EPSG:4326)
    coords = [(-20, 30), (50, 30), (50, 80), (-20, 80), (-20, 30)]
    poly_degrees = Polygon(coords)
    
    # 2. Create the GeoDataFrame in Lat/Lon
    fake_world = gpd.GeoDataFrame(
        {
            "name": ["United Kingdom", "Luxembourg", "Bosnia and Herz.", "Croatia", "Hungary", "Romania", "Bulgaria", "North Macedonia", "Kosovo", "Albania", "Montenegro", "Serbia"], 
            "NAME": ["United Kingdom", "Luxembourg", "Bosnia and Herz.", "Croatia", "Hungary", "Romania", "Bulgaria", "North Macedonia", "Kosovo", "Albania", "Montenegro", "Serbia"],
            "geometry": [poly_degrees] * 12 
        }, 
        crs="EPSG:4326"
    )

    # 3. CRITICAL: Convert to Meters (EPSG:4087)
    # The real code converts the map to meters before using it. 
    # If we skip this, buffer(50000) tries to create a buffer of 50,000 DEGREES, which crashes the geometry engine.
    fake_world_meters = fake_world.to_crs("EPSG:4087")

    # 4. Patch the method that triggers the download
    # We patch 'get_hires_world_boundaries' to return our fake data
    mock_method = mocker.patch(
        "pvoutput.grid_search.natural_earth.NaturalEarth.get_hires_world_boundaries"
    )
    
    # Return: (Geodataframe in METERS, List of country names)
    mock_method.return_value = (fake_world_meters, list(fake_world.name))
    
    return mock_method
