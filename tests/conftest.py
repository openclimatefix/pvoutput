import inspect
import tempfile
import yaml
import os
import pickle
from functools import partial

import pytest

from pvoutput import mapscraper as ms
from pvoutput.consts import CONFIG_FILENAME
from pvoutput.pvoutput import _get_param_from_config_file


@pytest.fixture
def data_dir():
    # Taken from http://stackoverflow.com/a/6098238/732596
    data_dir = os.path.dirname(inspect.getfile(inspect.currentframe()))
    data_dir = os.path.abspath(data_dir)
    assert os.path.isdir(data_dir), data_dir + " does not exist."
    return data_dir


@pytest.fixture
def fake_configuration():
    configuration = {'api_key': 'wrong_key',
                    'system_id': 68732}

    with tempfile.TemporaryDirectory() as tmpdirname:
        filename = tmpdirname + '/config.yaml'
        with open(filename, 'w') as outfile:
            yaml.dump(configuration, outfile, default_flow_style=False)

        yield filename


@pytest.fixture
def real_configuration():
    configuration = {}

    for key in ['api_key', 'system_id']:
        try:
            value = os.environ[key.upper()]
        except:
            value = _get_param_from_config_file(
                key, CONFIG_FILENAME
            )
        configuration[key] = value

    with tempfile.TemporaryDirectory() as tmpdirname:
        filename = tmpdirname + '/config.yaml'
        with open(filename, 'w') as outfile:
            yaml.dump(configuration, outfile, default_flow_style=False)

        yield filename


def get_cleaned_test_soup(data_dir):
    test_soup_filepath = os.path.join(data_dir, "data/mapscraper_soup.pickle")
    with open(test_soup_filepath, "rb") as f:
        test_soup = pickle.load(f)
    return ms.clean_soup(test_soup)


@pytest.fixture()
def get_test_dict_of_dfs(data_dir):
    dict_filepath = os.path.join(data_dir, "data/mapscraper_dict_of_dfs.pickle")
    with open(dict_filepath, "rb") as f:
        test_soup = pickle.load(f)
    return test_soup


@pytest.fixture()
def get_function_dict(data_dir):
    # using partials so functions only get executed when needed
    soup = get_cleaned_test_soup(data_dir)
    df = ms._process_system_size_col(soup)
    index = df.index
    keys = get_keys_for_dict()
    functions = (
        partial(ms._process_system_size_col, soup),
        partial(ms._process_output_col, soup, index),
        partial(ms._process_generation_and_average_cols, soup, index),
        partial(ms._process_efficiency_col, soup, index),
        partial(ms._process_metadata, soup),
    )
    function_dict = dict(zip(keys, functions))
    return function_dict


def get_keys_for_dict():
    keys = (
        "pv_system_size_metadata",
        "process_output_col",
        "process_generation_and_average_cols",
        "process_efficiency_col",
        "process_metadata",
    )
    return keys