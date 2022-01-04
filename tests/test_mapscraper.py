import pandas as pd
import pytest

from pvoutput import mapscraper as ms
from pvoutput.consts import MAP_URL


def compare_function_output_to_pickle(key, function_dict, dict_of_dfs, series=False):
    df_from_func = function_dict[key]()
    test_df = dict_of_dfs[key]
    if series:
        return pd.testing.assert_series_equal(df_from_func, test_df)
    return pd.testing.assert_frame_equal(df_from_func, test_df, check_like=True)


def test_convert_to_country_code():
    assert ms._convert_to_country_code(1) == 1
    assert ms._convert_to_country_code("United Kingdom") == 243

    def _assert_raises(bad_countries, exception):
        for bad_country in bad_countries:
            with pytest.raises(exception):
                ms._convert_to_country_code(bad_country)
                pytest.fail(
                    "Failed to raise {} for country={}".format(exception.__name__, bad_country)
                )

    _assert_raises([-1, -100, 1000, "blah"], ValueError)


def test_create_map_url():
    assert ms._create_map_url() == MAP_URL
    assert ms._create_map_url(country_code=1) == MAP_URL + "?country=1"
    assert ms._create_map_url(page_number=2) == MAP_URL + "?p=2"
    assert ms._create_map_url(ascending=True) == MAP_URL + "?d=asc"
    assert ms._create_map_url(ascending=False) == MAP_URL + "?d=desc"
    assert ms._create_map_url(sort_by="efficiency") == MAP_URL + "?o=gss"
    with pytest.raises(ValueError):
        ms._create_map_url(sort_by="blah")


def test_pv_system_size_metadata(get_function_dict, get_test_dict_of_dfs):
    assert (
        compare_function_output_to_pickle(
            "pv_system_size_metadata", get_function_dict, get_test_dict_of_dfs
        )
        is None
    )


def test_process_output_col(get_function_dict, get_test_dict_of_dfs):
    assert (
        compare_function_output_to_pickle(
            "process_output_col", get_function_dict, get_test_dict_of_dfs, series=True
        )
        is None
    )


def test_process_generation_and_average_cols(get_function_dict, get_test_dict_of_dfs):
    assert (
        compare_function_output_to_pickle(
            "process_generation_and_average_cols", get_function_dict, get_test_dict_of_dfs
        )
        is None
    )


def test_process_efficiency_col(get_function_dict, get_test_dict_of_dfs):
    assert (
        compare_function_output_to_pickle(
            "process_efficiency_col", get_function_dict, get_test_dict_of_dfs, series=True
        )
        is None
    )


def test_process_metadata(get_function_dict, get_test_dict_of_dfs):
    assert (
        compare_function_output_to_pickle(
            "process_metadata", get_function_dict, get_test_dict_of_dfs
        )
        is None
    )
