import os
import pickle
import sys

from pvoutput import mapscraper as ms
from pvoutput.tests.test_utils import data_dir


FILENAME = 'mapscraper_dict_of_dfs.pickle'


def get_keys_for_dict():
    keys = ('pv_system_size_metadata', 'process_output_col',
            'process_generation_and_average_cols', 'process_efficiency_col',
            'process_metadata')
    return keys


def get_test_soup():
    test_soup_filepath = os.path.join(data_dir(), 'mapscraper_soup.pickle')
    with open(test_soup_filepath, 'rb') as f:
        test_soup = pickle.load(f)
    return test_soup


def main():
    soup = get_test_soup()
    keys = get_keys_for_dict()
    values =  ms._process_metadata(soup, True)
    df_dict = dict(zip(keys, values))
    #needed to avoid occasional RecursionError
    sys.setrecursionlimit(10000)
    with open(FILENAME, 'wb') as f:
        pickle.dump(df_dict, f)


if __name__ == '__main__':
    main()