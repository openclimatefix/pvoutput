import pickle
import sys

from pvoutput import mapscraper as ms


def get_keys_for_dict():
    keys = (
        "pv_system_size_metadata",
        "process_output_col",
        "process_generation_and_average_cols",
        "process_efficiency_col",
        "process_metadata",
    )
    return keys


def save_pickle_test_file(file, filename):
    # needed to avoid occasional RecursionError
    sys.setrecursionlimit(10000)
    with open(filename, "wb") as f:
        pickle.dump(file, f)


def get_raw_soup():
    url = ms._create_map_url(country_code=243, page_number=1, ascending=False, sort_by="capacity")
    return ms.get_soup(url, raw=True)


def main():
    raw_soup = get_raw_soup()
    save_pickle_test_file(raw_soup, "mapscraper_soup.pickle")
    soup = ms.clean_soup(raw_soup)
    keys = get_keys_for_dict()
    values = ms._process_metadata(soup, True)
    df_dict = dict(zip(keys, values))
    save_pickle_test_file(df_dict, "mapscraper_dict_of_dfs.pickle")


if __name__ == "__main__":
    main()
