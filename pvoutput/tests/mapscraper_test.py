import pytest
import numpy as np
import os
import pickle

from pvoutput import mapscraper as ms
from pvoutput.consts import MAP_URL
from pvoutput.tests.test_utils import data_dir

#todo get pickle file which is a dict of dataframes for all the mapscraper functions and create a fixture for that
#TODO write unit test for each function make sure it matches input df including for _process_metadata
#TODO add bit of code below in _process_metadata, make sure it still passes
# for script in soup.find_all('script', src=False):
#     script.decompose()

#TODO CHECK FIXTURE ONLY CALLED ONCE


@pytest.fixture
def get_test_soup():
    test_soup_filepath = os.path.join(data_dir(), 'mapscraper_soup.pickle')
    with open(test_soup_filepath, 'rb') as f:
        test_soup = pickle.load(f)
    return test_soup


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


def test_create_map_url():
    assert ms._create_map_url() == MAP_URL
    assert ms._create_map_url(country_code=1) == MAP_URL + '?country=1'
    assert ms._create_map_url(page_number=2) == MAP_URL + '?p=2'
    assert ms._create_map_url(ascending=True) == MAP_URL + '?d=asc'
    assert ms._create_map_url(ascending=False) == MAP_URL + '?d=desc'
    assert ms._create_map_url(sort_by='efficiency') == MAP_URL + '?o=gss'
    with pytest.raises(ValueError):
        ms._create_map_url(sort_by='blah')
