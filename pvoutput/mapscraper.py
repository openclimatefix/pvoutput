import re
from copy import copy
from typing import Iterable, Optional, Union

import pandas as pd
import requests
from bs4 import BeautifulSoup

from pvoutput.consts import (
    MAP_URL,
    PV_OUTPUT_COUNTRY_CODES,
    PV_OUTPUT_MAP_COLUMN_NAMES,
    REGIONS_URL,
)

_MAX_NUM_PAGES = 1024


def get_pv_systems_for_country(
    country: Union[str, int],
    ascending: Optional[bool] = None,
    sort_by: Optional[str] = None,
    max_pages: int = _MAX_NUM_PAGES,
    region: Optional[str] = None,
) -> pd.DataFrame:
    """
    Args:
        country: either a string such as 'United Kingdom'
            (see consts.PV_OUTPUT_COUNTRY_CODES for all recognised strings),
            or a PVOutput.org country code, in the range [1, 257].
        ascending: if True, ask PVOutput.org to sort results by ascending.
            If False, sort by descending.  If None, use PVOutput.org's default
            sort order.
        sort_by: The column to ask PVOutput.org to sort by.  One of:
            timeseries_duration,
            average_generation_per_day,
            efficiency,
            power_generation,
            capacity,
            address,
            name
        max_pages: The maximum number of search pages to scrape.

    Returns: pd.DataFrame with index system_id (int) and these columns:
        name, system_DC_capacity_W, panel, inverter, address, orientation,
        array_tilt_degrees, shade, timeseries_duration,
        total_energy_gen_Wh, average_daily_energy_gen_Wh
        average_efficiency_kWh_per_kW
    """

    country_code = _convert_to_country_code(country)
    regions = [region] if region else get_regions_for_country(country_code)
    all_metadata = []
    for region in regions:
        for page_number in range(max_pages):
            print(
                "\rReading page {:2d} for region: {}".format(page_number, region),
                end="",
                flush=True,
            )
            url = _create_map_url(
                country_code=country_code,
                page_number=page_number,
                ascending=ascending,
                sort_by=sort_by,
                region=region,
            )
            soup = get_soup(url)
            if _page_is_blank(soup):
                break
            metadata = _process_metadata(soup)
            metadata["region"] = region
            all_metadata.append(metadata)

            if not _page_has_next_link(soup):
                break

    return pd.concat(all_metadata)


############ LOAD HTML ###################


def _create_map_url(
    country_code: Optional[int] = None,
    page_number: Optional[int] = None,
    ascending: Optional[bool] = None,
    sort_by: Optional[str] = None,
    region: Optional[str] = None,
) -> str:
    """
    Args:
        page_number: Get this page number of the search results.  Zero-indexed.
            The first page is page 0, the second page is page 1, etc.
    """
    _check_country_code(country_code)

    if ascending is None:
        sort_order = None
    else:
        sort_order = "asc" if ascending else "desc"

    if sort_by is None:
        sort_by_pv_output_col_name = None
    else:
        try:
            sort_by_pv_output_col_name = PV_OUTPUT_MAP_COLUMN_NAMES[sort_by]
        except KeyError:
            raise ValueError("sort_by must be one of {}".format(PV_OUTPUT_MAP_COLUMN_NAMES.keys()))

    url_params = {
        "country": country_code,
        "p": page_number,
        "d": sort_order,
        "o": sort_by_pv_output_col_name,
        "region": region,
    }

    url_params_list = [
        "{}={}".format(key, value) for key, value in url_params.items() if value is not None
    ]
    query_string = "&".join(url_params_list)
    url = copy(MAP_URL)
    if query_string:
        url += "?" + query_string
    return url


def _raise_country_error(country, msg=""):
    country_codes = PV_OUTPUT_COUNTRY_CODES.values()
    raise ValueError(
        "Wrong value country='{}'. {}country must be an integer country"
        " code in the range [{}, {}], or one of {}.".format(
            country,
            msg,
            min(country_codes),
            max(country_codes),
            ", ".join(PV_OUTPUT_COUNTRY_CODES.keys()),
        )
    )


def _check_country_code(country_code: Union[None, int]):
    if country_code is None:
        return
    country_codes = PV_OUTPUT_COUNTRY_CODES.values()
    if not min(country_codes) <= country_code <= max(country_codes):
        _raise_country_error(country_code, "country outside of valid range!  ")


def _convert_to_country_code(country: Union[str, int]) -> int:
    if isinstance(country, str):
        try:
            return PV_OUTPUT_COUNTRY_CODES[country]
        except KeyError:
            _raise_country_error(country)

    elif isinstance(country, int):
        _check_country_code(country)
        return country


def _page_has_next_link(soup: BeautifulSoup):
    return bool(soup.find_all("a", text="Next"))


############# PROCESS HTML #########################


def _process_metadata(soup: BeautifulSoup, return_constituents=False) -> pd.DataFrame:
    pv_system_size_metadata = _process_system_size_col(soup)
    index = pv_system_size_metadata.index
    pv_systems_metadata = [
        pv_system_size_metadata,
        _process_output_col(soup, index),
        _process_generation_and_average_cols(soup, index),
        _process_efficiency_col(soup, index),
    ]

    df = pd.concat(pv_systems_metadata, axis="columns")
    df = _convert_metadata_cols_to_numeric(df)
    df["system_DC_capacity_W"] = df["capacity_kW"] * 1e3
    del df["capacity_kW"]
    if return_constituents:
        pv_systems_metadata.append(df)
        return tuple(pv_systems_metadata)
    return df


def _process_system_size_col(soup: BeautifulSoup) -> pd.DataFrame:
    pv_system_size_col = soup.find_all("a", href=re.compile("display\.jsp\?sid="))
    metadata = []
    for row in pv_system_size_col:
        metadata_for_row = {}

        # Get system ID
        href = row.attrs["href"]
        p = re.compile("^display\.jsp\?sid=(\d+)$")
        href_match = p.match(href)
        metadata_for_row["system_id"] = href_match.group(1)

        # Process title (lots of metadata in here!)
        title, title_meta = row.attrs["title"].split("|")

        # Name and capacity
        p = re.compile("(.*) (\d+\.\d+kW)")
        title_match = p.match(title)
        metadata_for_row["name"] = title_match.group(1)
        metadata_for_row["capacity"] = title_match.group(2)

        # Other key-value pairs:
        key_value = title_meta.split("<br/>")
        key_value_dict = {}
        for line in key_value:
            key_value_split = line.split(":")
            key = key_value_split[0].strip()
            # Some values have a colon(!)
            value = ":".join(key_value_split[1:]).strip()
            key_value_dict[key] = value
        metadata_for_row.update(key_value_dict)

        # Some cleaning
        # Remove <img ...> from Location
        location = metadata_for_row["Location"]
        p = re.compile("(<img .*\>)?(.*)")
        img_groups = p.search(location).groups()
        if img_groups[0] is not None:
            metadata_for_row["Location"] = img_groups[1].strip()

        metadata.append(metadata_for_row)

    df = pd.DataFrame(metadata)
    df["system_id"] = pd.to_numeric(df["system_id"])
    df = df.set_index("system_id")
    df.columns = [col_name.lower() for col_name in df.columns]
    df.rename(
        {
            "location": "address",
            "panels": "panel",
            "array tilt": "array_tilt_degrees",
            "capacity": "capacity_kW",
        },
        axis="columns",
        inplace=True,
    )
    return df


def _remove_str_and_convert_to_numeric(series: pd.Series, string_to_remove: str) -> pd.Series:
    series = series.str.replace(string_to_remove, "")
    return pd.to_numeric(series)


def _convert_metadata_cols_to_numeric(df: pd.DataFrame) -> pd.DataFrame:
    for col_name, string_to_remove in [
        # ('array_tilt_degrees', '°'),
        ("capacity_kW", "kW"),
        ("average_efficiency_kWh_per_kW", "kWh/kW"),
    ]:
        df[col_name] = _remove_str_and_convert_to_numeric(df[col_name], string_to_remove)

    return df


def _process_output_col(soup: BeautifulSoup, index: Optional[Iterable] = None) -> pd.Series:

    # get all data
    outputs_col = soup.find_all(text=re.compile("\d Days"))

    # format data as strings
    outputs_col = [str(col) for col in outputs_col]

    # make into pandas Series
    duration = pd.Series(outputs_col, name="timeseries_duration", index=index)

    # change to timedeltas
    return pd.to_timedelta(duration.astype("unicode"))


def _convert_energy_to_numeric_watt_hours(series: pd.Series) -> pd.Series:
    data = []
    for unit, multiplier in [("kWh", 1e3), ("MWh", 1e6)]:
        selection = series[series.str.contains(unit)]
        selection = selection.str.replace(unit, "")
        selection = pd.to_numeric(selection)
        selection *= multiplier
        data.append(selection)
    return pd.concat(data)


def _process_generation_and_average_cols(
    soup: BeautifulSoup, index: Optional[Iterable] = None
) -> pd.DataFrame:
    # _soup = deepcopy(soup)
    [s.decompose() for s in soup.select("a")]
    generation_and_average_cols = soup.find_all(text=re.compile("\d[Mk]Wh$"))
    generation_col = generation_and_average_cols[0::2]
    average_col = generation_and_average_cols[1::2]
    df = pd.DataFrame(
        {"total_energy_gen_Wh": generation_col, "average_daily_energy_gen_Wh": average_col},
        index=index,
    )

    for col_name in df.columns:
        df[col_name] = _convert_energy_to_numeric_watt_hours(df[col_name])

    return df


def _process_efficiency_col(soup: BeautifulSoup, index: Optional[Iterable] = None) -> pd.Series:
    efficiency_col = soup.find_all(text=re.compile("\dkWh/kW"))
    return pd.Series(efficiency_col, name="average_efficiency_kWh_per_kW", index=index)


def _page_is_blank(soup: BeautifulSoup) -> bool:
    # Pages can still be blank even if the previous page has a Next Button
    pv_system_size_col = soup.find_all("a", href=re.compile("display\.jsp\?sid="))
    return not bool(pv_system_size_col)


def get_soup(url, raw=False, parser="html.parser"):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, parser)
    if raw:
        return soup
    return clean_soup(soup)


def clean_soup(soup):
    """Function to clean scraped soup object. The downloaded soup could change
        over time.
    Args:
        soup: bs4.BeautifulSoup

    Returns:
        bs4.BeautifulSoup

    """
    for script in soup.find_all("script", src=False):
        script.decompose()
    return soup


def get_regions_for_country(country_code: int):
    region_list = []
    url = f"{REGIONS_URL}?country={country_code}"
    soup = get_soup(url, parser="lxml")
    region_tags = soup.find_all("a", href=re.compile("map\.jsp\?country="))
    for row in region_tags:
        href = row.attrs["href"]
        p = re.compile("^map\.jsp\?country=243&region=(\w+.*)$")
        href_match = p.match(href)
        region = href_match.group(1)
        region_list.append(region)
    return region_list
