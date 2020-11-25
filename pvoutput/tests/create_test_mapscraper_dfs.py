import pickle
import sys

from pvoutput import mapscraper as ms
from pvoutput.tests.mapscraper_test import get_keys_for_dict, get_test_soup


FILENAME = 'mapscraper_dict_of_dfs.pickle'


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