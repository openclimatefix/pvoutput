import os

from pvoutput.grid_search.grid_search import GridSearch

SHOW = True
if "CI" in os.environ:
    SHOW = False


def test_init():
    """Test that Grid search can be initiated"""
    _ = GridSearch()


def test_list_countries():
    """Get list of countries"""
    grd = GridSearch()
    countries = grd.nat_earth.list_countries()
    assert len(countries) == 258


def test_uk_grid():
    """Example 1: Get UK grid

    Use this to clip to a bounding box as well as the countries selected
    List as many countries as you want, or set to None for world-wide
    Only include search points within a certain radius of a location (see Example 3)
    Increase this if you'd like to consider systems "near" the target region (see Example 2)
    Allow some extra overlap due to inaccuracies in measuring distance
    EPSG:27700 is OSGB36 / British National Grid
    Gives a nice plot of the region and grid
    """
    grd = GridSearch()
    ukgrid = grd.generate_grid(
        bbox=[45, -15, 58, 15],
        countries=["United Kingdom"],
        radial_clip=None,
        buffer=0,
        search_radius=24.5,
        local_crs_epsg=27700,
        show=SHOW,
    )
    assert len(ukgrid) > 100


def test_luxembourg_grid():
    """Example 2: Make Luxembourg grid

    Include search radii within 50km of Luzembourgs border
    Allow some extra overlap due to inaccuracies in measuring distance
    EPSG:2169 is Luxembourg 1930 / Gauss

    """
    grd = GridSearch()
    luxgrid = grd.generate_grid(
        countries=["Luxembourg"], buffer=50, search_radius=24.5, local_crs_epsg=2169, show=SHOW
    )
    luxgrid.head()
    assert len(luxgrid) == 18


def test_sheffield_grid():
    """Make grid around Sheffield

    Only include search points within a 100km of the TUOS Physics Department
    EPSG:27700 is OSGB36 / British National Grid

    """
    grd = GridSearch()
    shefgrid = grd.generate_grid(
        radial_clip=(
            53.381,
            -1.486,
            100.0,
        ),  # Only include search points within a 100km of the TUOS Physics Department
        local_crs_epsg=27700,  # EPSG:27700 is OSGB36 / British National Grid
        show=SHOW,
    )
    assert len(shefgrid) == 29


def test_balkan_grid():
    """Plot grid around Balkan area"""
    grd = GridSearch()
    balkan_grid = grd.generate_grid(
        countries=[
            "Bosnia and Herz.",
            "Croatia",
            "Hungary",
            "Romania",
            "Bulgaria",
            "North Macedonia",
            "Kosovo",
            "Albania",
            "Montenegro",
            "Serbia",
        ],
        search_radius=24.5,
        show=SHOW,
    )
    assert len(balkan_grid) == 733
