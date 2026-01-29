from pvoutput.grid_search import GridSearch

SHOW = False


def test_init():
    """Test init"""
    _ = GridSearch()


def test_list_countries(mock_natural_earth):
    """Get list of countries"""
    grd = GridSearch()
    countries = grd.nat_earth.list_countries()
    assert len(countries) > 0
    assert "United Kingdom" in countries


def test_uk_grid(mock_natural_earth):
    """Example 1: Get UK grid"""
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
    assert not ukgrid.empty


def test_luxembourg_grid(mock_natural_earth):
    """Example 2: Make Luxembourg grid"""
    grd = GridSearch()
    luxgrid = grd.generate_grid(
        countries=["Luxembourg"], buffer=50, search_radius=24.5, local_crs_epsg=2169, show=SHOW
    )
    assert not luxgrid.empty


def test_sheffield_grid(mock_natural_earth):
    """Make grid around Sheffield"""
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
    assert not shefgrid.empty


def test_balkan_grid(mock_natural_earth):
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
    assert not balkan_grid.empty
