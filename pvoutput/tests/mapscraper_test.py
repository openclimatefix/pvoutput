import pytest
import numpy as np
from pvoutput import mapscraper as ms
from pvoutput.consts import MAP_URL
from pytypes import InputTypeError


def test_convert_to_country_code():
    assert ms._convert_to_country_code(1) == 1
    assert ms._convert_to_country_code('United Kingdom') == 243

    def _assert_raises(bad_countries, exception):
        for bad_country in bad_countries:
            with pytest.raises(exception):
                ms._convert_to_country_code(bad_country)
                pytest.fail('Failed to raise {} for country={}'
                            .format(exception.__name__, bad_country))

    _assert_raises([-1, -100, 1000, 'blah'], ValueError)
    _assert_raises([1.5, np.inf, np.NaN], InputTypeError)


def test_create_map_url():
    assert ms._create_map_url() == MAP_URL
    assert ms._create_map_url(country_code=1) == MAP_URL + '?country=1'
    assert ms._create_map_url(page_number=2) == MAP_URL + '?p=2'
    assert ms._create_map_url(ascending=True) == MAP_URL + '?d=asc'
    assert ms._create_map_url(ascending=False) == MAP_URL + '?d=desc'
    assert ms._create_map_url(sort_by='efficiency') == MAP_URL + '?o=gss'
    with pytest.raises(ValueError):
        ms._create_map_url(sort_by='blah')
