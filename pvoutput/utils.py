import logging
import os
import sys
import warnings
from datetime import date, datetime
from typing import Dict, Iterable, List, Union

import numpy as np
import pandas as pd
import requests
import tables
import yaml
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from pvoutput.consts import CONFIG_FILENAME
from pvoutput.daterange import DateRange, get_date_range_list

_LOG = logging.getLogger("pvoutput")


def _get_param_from_config_file(param_name, config_filename=CONFIG_FILENAME):
    with open(config_filename, mode="r") as fh:
        config_data = yaml.load(fh, Loader=yaml.Loader)
    try:
        value = config_data[param_name]
    except KeyError as e:
        print("Config file", config_filename, "does not contain a", param_name, "parameter.", e)
        raise
    return value


def get_logger(filename=None, mode="a", level=logging.DEBUG, stream_handler=False):
    if filename is None:
        filename = _get_param_from_config_file("log_filename")
    logger = logging.getLogger("pvoutput")
    logger.setLevel(level)
    logger.handlers = [logging.FileHandler(filename=filename, mode=mode)]
    if stream_handler:
        logger.handlers.append(logging.StreamHandler(sys.stdout))
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    for handler in logger.handlers:
        handler.setFormatter(formatter)

    # Attach urllib3's logger to our logger.
    loggers_to_attach = ["urllib3", "requests"]
    for logger_name_to_attach in loggers_to_attach:
        logger_to_attach = logging.getLogger(logger_name_to_attach)
        logger_to_attach.parent = logger
        logger_to_attach.propagate = True

    return logger


def _get_session_with_retry() -> requests.Session:
    max_retry_counts = dict(
        connect=720,  # How many connection-related errors to retry on.
        # Set high because sometimes the network goes down for a
        # few hours at a time.
        # 720 x Retry.MAX_BACKOFF (120 s) = 86,400 s = 24 hrs
        read=20,  # How many times to retry on read errors.
        status=20,  # How many times to retry on bad status codes.
    )
    retries = Retry(
        total=max(max_retry_counts.values()),
        backoff_factor=0.5,
        status_forcelist=[500, 502, 503, 504],
        **max_retry_counts
    )
    session = requests.Session()
    session.mount("http://", HTTPAdapter(max_retries=retries))
    session.mount("https://", HTTPAdapter(max_retries=retries))
    return session


def _get_response(api_url: str, api_params: Dict, headers: Dict) -> requests.Response:
    api_params_str = "&".join(["{}={}".format(key, value) for key, value in api_params.items()])
    full_api_url = "{}?{}".format(api_url, api_params_str)
    session = _get_session_with_retry()
    response = session.get(full_api_url, headers=headers)
    _LOG.debug("response: status_code=%d; headers=%s", response.status_code, response.headers)
    return response


def _print_and_log(msg: str, level: int = logging.INFO):
    _LOG.log(level, msg)
    print(msg)


def get_system_ids_in_store(store_filename: str) -> List[int]:
    if not os.path.exists(store_filename):
        return []
    with pd.HDFStore(store_filename, mode="r") as store:
        pv_system_ids = list(store.walk("/timeseries"))[0][2]
    return pd.to_numeric(pv_system_ids)


def get_date_ranges_to_download(
    store_filename: str,
    system_id: int,
    start_date: Union[str, datetime],
    end_date: Union[str, datetime],
) -> List[DateRange]:
    """If system_id in store, check if it already has data from
    start_date to end_date, taking into consideration missing_dates.

    Returns: list of DateRange objects
        For each DateRange we need to download from
        start_date to end_date inclusive.
    """
    dates_to_download = list(pd.date_range(start_date, end_date, freq="D"))
    dates_to_download = datetime_list_to_dates(dates_to_download)
    dates_already_downloaded = get_dates_already_downloaded(store_filename, system_id)
    dates_to_download = set(dates_to_download) - set(dates_already_downloaded)
    missing_dates_for_id = get_missing_dates_for_id(store_filename, system_id)
    dates_to_download -= set(missing_dates_for_id)
    return get_date_range_list(list(dates_to_download))


def get_missing_dates_for_id(store_filename: str, system_id: int) -> List:
    if not os.path.exists(store_filename):
        return []

    with pd.HDFStore(store_filename, mode="r") as store:
        missing_dates_for_id = store.select(
            key="missing_dates",
            where="index=system_id",
            columns=["missing_start_date_PV_localtime", "missing_end_date_PV_localtime"],
        )

    missing_dates = []
    for _, row in missing_dates_for_id.iterrows():
        missing_date_range = pd.date_range(
            row["missing_start_date_PV_localtime"], row["missing_end_date_PV_localtime"], freq="D"
        ).tolist()
        missing_dates.extend(missing_date_range)

    missing_dates = np.sort(np.unique(missing_dates))
    missing_dates = datetime_list_to_dates(missing_dates)
    print()
    _LOG.info("system_id %d: %d missing dates already found", system_id, len(missing_dates))
    return missing_dates


def datetime_list_to_dates(datetimes: Iterable[datetime]) -> Iterable[date]:
    if not isinstance(datetimes, Iterable):
        datetimes = [datetimes]
    return pd.DatetimeIndex(datetimes).date


def get_dates_already_downloaded(store_filename, system_id) -> set:
    if not os.path.exists(store_filename):
        return set([])

    with pd.HDFStore(store_filename, mode="r") as store:
        key = system_id_to_hdf_key(system_id)
        try:
            datetimes = store.select(key=key, columns=["datetime", "query_date"])
        except KeyError:
            return set([])
        else:
            query_dates = datetime_list_to_dates(datetimes["query_date"].dropna())
            return set(datetimes.index.date).union(query_dates)


def system_id_to_hdf_key(system_id: int) -> str:
    return "/timeseries/{:d}".format(system_id)


def sort_and_de_dupe_pv_system(store, pv_system_id):
    key = system_id_to_hdf_key(pv_system_id)
    timeseries = store[key]
    timeseries.sort_index(inplace=True)
    timeseries = timeseries[~timeseries.index.duplicated()]
    store.remove(key)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", tables.NaturalNameWarning)
        store.append(key, timeseries, data_columns=True)
