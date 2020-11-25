import os
import pickle

from pvoutput import mapscraper as ms
from pvoutput.tests.mapscraper_test import get_test_soup, data_dir


FILENAME = 'mapscraper_dfs.pickle'


def get_test_soup():
    test_soup_filepath = os.path.join(data_dir(), 'mapscraper_soup.pickle')
    with open(test_soup_filepath, 'rb') as f:
        test_soup = pickle.load(f)
    return test_soup


def main():
    keys = ('pv_system_size_metadata', 'process_output_col',
            'process_generation_and_average_cols', 'process_efficiency_col',
            'combined_metadata')
    soup = get_test_soup()
    values =  ms._process_metadata(soup, True)
    df_dict = dict(zip(keys, values))
    with open(FILENAME, 'wb') as f:
        pickle.dump(df_dict, f)


if __name__ == '__main__':
    main()