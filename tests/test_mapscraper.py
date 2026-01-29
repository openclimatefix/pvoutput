import pandas as pd
import pytest
from pvoutput import mapscraper as ms
from pvoutput.consts import MAP_URL


# --- URL Generation Tests ---
def test_convert_to_country_code():
    assert ms._convert_to_country_code(1) == 1
    assert ms._convert_to_country_code("United Kingdom") == 243

    with pytest.raises(ValueError):
        ms._convert_to_country_code(-1)


def test_create_map_url():
    assert ms._create_map_url() == MAP_URL
    assert ms._create_map_url(country_code=1) == MAP_URL + "?country=1"


# --- Data Extraction Tests (Using your mock HTML file) ---


def test_pv_system_size_metadata(map_soup):
    df = ms._process_system_size_col(map_soup)
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    # Corrected column name based on actual code output
    assert "capacity_kW" in df.columns


def test_process_output_col(map_soup):
    series = ms._process_output_col(map_soup)
    assert isinstance(series, pd.Series)
    assert not series.empty


def test_process_generation_and_average_cols(map_soup):
    df = ms._process_generation_and_average_cols(map_soup)
    assert isinstance(df, pd.DataFrame)
    # Corrected column name based on actual code output
    assert "total_energy_gen_Wh" in df.columns


def test_process_efficiency_col(map_soup):
    series = ms._process_efficiency_col(map_soup)
    assert isinstance(series, pd.Series)


def test_process_metadata(map_soup):
    df = ms._process_metadata(map_soup)
    assert isinstance(df, pd.DataFrame)
    # Corrected column name based on actual code output
    assert "address" in df.columns
