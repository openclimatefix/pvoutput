"""PVOutput.org utils

## How to setup this notebook

1. Register with PVOutput.org
      As well as an API key, you *need* a SystemId to use the API.  
      If you don't include a SystemId, then you'll get a "401 Unauthorized" response
      from the PVOutput.org API.  If you don't have a PV system, click the
      "energy consumption only" box when setting a system id on PVOutput.org.

2. Set the environment variables PVOUTPUT_APIKEY and PVOUTPUT_SYSTEMID 
      (on Linux, put `EXPORT PVOUTPUT_APIKEY="API KEY"` etc. into `.profile`, 
      log out, and log back in again)
"""
from io import StringIO
import sys
import os
import re
import time
import logging
from datetime import datetime, timedelta
import urllib3
import requests
import numpy as np
import pandas as pd

SECONDS_PER_DAY = 60 * 60 * 24
ONE_DAY = timedelta(days=1)
PV_OUTPUT_DATE_FORMAT="%Y%m%d"

def get_logger(filename='/home/jack/data/pvoutput.org/logs/UK_PV_timeseries.log', 
               mode='a', 
               level=logging.DEBUG,
               stream_handler=False):
    logger = logging.getLogger('pv_output')
    logger.setLevel(level)
    logger.handlers = [logging.FileHandler(filename=filename, mode=mode)]
    if stream_handler:
        logger.handlers.append(logging.StreamHandler(sys.stdout))
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    for handler in logger.handlers:
        handler.setFormatter(formatter)
    return logger


logger = get_logger()


class BadStatusCode(Exception):
    def __init__(self, response, message=''):
        self.response = response
        super(BadStatusCode, self).__init__(message)
        
    def __str__(self):
        string = super(BadStatusCode, self).__str__()
        string += "Status code returned: {}\n".format(self.response.status_code)
        string += "Response content: {}\n".format(self.response.content)
        string += "Response headers: {}".format(self.response.headers)
        return string


class NoStatusFound(BadStatusCode):
    pass


class RateLimitExceeded(BadStatusCode):
    def __init__(self, *args, **kwargs):
        super(RateLimitExceeded, self).__init__(*args, **kwargs)
        self._set_params()

    def _set_params(self):
        self.utc_now = datetime.utcnow()
        self.rate_limit_reset_datetime = datetime.utcfromtimestamp(
            int(self.response.headers['X-Rate-Limit-Reset']))
        self.timedelta_to_wait = self.rate_limit_reset_datetime - self.utc_now
        self.timedelta_to_wait += timedelta(minutes=3) # Just for safety
        self.secs_to_wait = self.timedelta_to_wait.total_seconds()
        
    def __str__(self):
        return 'Rate limit exceeded!'

    def wait_message(self):
        retry_time_utc = self.utc_now + self.timedelta_to_wait
        return '{}  Waiting {:.0f} seconds.  Will retry at {} UTC.'.format(
            self, self.secs_to_wait,
            retry_time_utc.strftime('%Y-%m-%d %H:%M:%S'))


def _get_api_reponse(service, api_params):
    """
    Args:
        service: string, e.g. 'search', 'getstatus'
        api_params: dict
    """   
    headers = {
        'X-Pvoutput-Apikey': os.environ['PVOUTPUT_APIKEY'],
        'X-Pvoutput-SystemId': os.environ['PVOUTPUT_SYSTEMID'],
        'X-Rate-Limit': '1'}

    api_base_url = 'https://pvoutput.org/service/r2/{}.jsp'.format(service)
    api_params_str = '&'.join(
        ['{}={}'.format(key, value) for key, value in api_params.items()])
    api_url = '{}?{}'.format(api_base_url, api_params_str)
    logger.debug(
        'service=%s\napi_params=%s\napi_url=%s\nheaders=%s',
        service, api_params, api_url, headers)
    response = requests.get(api_url, headers=headers)
    logger.debug(
        'response: status_code=%d; headers=%s',
        response.status_code, response.headers)
    return response


def _process_api_response(response):
    try:
        content = response.content.decode('latin1').strip()
    except UnicodeDecodeError as e:
        msg = "Error decoding this string: {}\n{}".format(response.content, e)
        print(msg)
        logger.exception(msg)
        raise

    if response.status_code == 400:
        raise NoStatusFound(response=response)
        
    # Did we overshoot our quota?
    rate_limit_remaining = int(response.headers['X-Rate-Limit-Remaining'])
    if response.status_code == 403 and rate_limit_remaining <= 0:
        raise RateLimitExceeded(response=response)

    if response.status_code != 200:
        raise BadStatusCode(response=response)

    # If we get to here then the content is valid :)
    return content


def pv_output_api_query(
        service, api_params, retries=150, seconds_between_retries=300,
        wait_if_rate_limit_exceeded=True):
    """
    Args:
        service: string, e.g. 'search' or 'getstatus'
        api_params: dict
        retries: int
        seconds_between_retries: int
        wait_if_rate_limit_exceeded: bool
    Raises:
        BadStatusCode
        NoStatusFound
        RateLimitExceeded
    """
    # TODO use Request's retry logic
    for retries_remaining in range(retries, -1, -1):
        try:
            response = _get_api_reponse(service, api_params)
        except urllib3.exceptions.HTTPError as e:
            logger.exception(
                'Exception!  Retries remaining: %d; Exception: %s: %s',
                retries_remaining, e.__class__.__name__, e)
            if retries_remaining == 0:
                raise
            else:
                logger.info('Waiting %d seconds and then retrying.',
                            seconds_between_retries)
                time.sleep(seconds_between_retries)
        else:
            break

    try:
        return _process_api_response(response)
    except RateLimitExceeded as e:
        if wait_if_rate_limit_exceeded:
            logger.info(e.wait_message())
            time.sleep(e.secs_to_wait)
            return pv_output_api_query(
                service, api_params, retries, seconds_between_retries,
                wait_if_rate_limit_exceeded=False)
        else:
            raise


def pv_system_search(query, lat_lon, **kwargs):
    """
    Args:
        query: string, see https://pvoutput.org/help.html#search
            e.g. '5km'.  Max search radius = 25km
        lat_lon: string, e.g. '52.0668589,-1.3484038'
        
    Returns:
        pd.DataFrame.  Beware the maximum number of results returned by PVOutput.org is 30.
    """
    
    pv_systems_text = pv_output_api_query(
        service='search',
        api_params={
            'q': query,
            'll': lat_lon,
            'country': 1  # country flag, whether or not to return country with the postcode
        }, **kwargs)
    
    pv_systems = pd.read_csv(
        StringIO(pv_systems_text),
        names=[
            'system_name',
            'system_size_watts',
            'postcode',
            'orientation',
            'num_outputs',
            'last_output',
            'system_id',
            'panel',
            'inverter',
            'distance_km',
            'latitude',
            'longitude'],
        index_col='system_id')

    return pv_systems


def date_to_pvoutput_str(date):
    if isinstance(date, str):
        return date
    else:
        return date.strftime(PV_OUTPUT_DATE_FORMAT)


def _check_date(date):
    dt = datetime.strptime(date, PV_OUTPUT_DATE_FORMAT)
    if dt > datetime.now():
        raise ValueError(
            'date should not be in the future.  Got {}.  Current date is {}.'
            .format(date, datetime.now()))
        

def get_pv_system_status(pv_system_id, date, **kwargs):
    """
    Args:
        pv_system_id: int
        date: str, YYYYMMDD
    """
    date = date_to_pvoutput_str(date)
    _check_date(date)
    
    pv_system_status_text = pv_output_api_query(
        service='getstatus',
        api_params={
            'd': date, # date, YYYYMMDD.
            'h': 1,  # We want historical data.
            'limit': 288,  # API docs say limit is 288 (number of 5-min periods per day).
            'ext': 0, # Extended data; we don't want extended data.
            'sid1': pv_system_id # SystemID.
        },
        **kwargs)
    
    columns = ['energy_generation_watt_hours',
            'energy_efficiency_kWh_per_kW',
            'inst_power_watt',
            'average_power_watt',
            'normalised_output',
            'energy_consumption_watt_hours',
            'power_consumption_watts',
            'temperature_celsius',
            'voltage']
    
    pv_system_status = pd.read_csv(
        StringIO(pv_system_status_text),
        lineterminator=';',
        names=['date', 'time'] + columns,
        parse_dates={'datetime': ['date', 'time']},
        index_col=['datetime'],
        dtype={col: np.float64 for col in columns}
    ).sort_index()
    
    return pv_system_status


def check_pv_system_status(pv_system_status, requested_date_str):
    if not isinstance(pv_system_status, pd.DataFrame):
        raise ValueError('pv_system_status must be a dataframe')
    requested_date = datetime.strptime(requested_date_str, "%Y%m%d").date()
    if len(pv_system_status) > 0:
        index = pv_system_status.index
        for d in [index[0], index[-1]]:
            if not (requested_date <= d.date() <= requested_date + ONE_DAY):
                  raise ValueError(
                      'A date in the index is outside the expected range.'
                      ' Date from index={}, requested_date={}'
                      .format(d, requested_date_str))


def get_pv_metadata(pv_system_id, **kwargs):
    """
    Args:
        pv_system_id: int
    """
    pv_metadata_text = pv_output_api_query(
        service='getsystem',
        api_params={
            'array2': 1,  # provide data about secondary array, if present
            'tariffs': 0,
            'teams': 0,
            'est': 0,
            'donations': 0,
            'sid1': pv_system_id, # SystemID
            'ext': 0, # extended data
        }, **kwargs)
    
    pv_metadata = pd.read_csv(
        StringIO(pv_metadata_text),
        lineterminator=';',
        names=[
            'system_name',
            'system_size_watts',
            'postcode',
            'number_of_panels',
            'panel_power_watts',
            'panel_brand',
            'num_inverters',
            'inverter_power_watts',
            'inverter_brand',
            'orientation',
            'array_tilt_degrees',
            'shade',
            'install_date',
            'latitude',
            'longitude',
            'status_interval_minutes',
            'number_of_panels_secondary',
            'panel_power_watts_secondary',
            'orientation_secondary',
            'array_tilt_degrees_secondary'
        ],
        parse_dates=['install_date'],
        nrows=1
    ).squeeze()
    pv_metadata['system_id'] = pv_system_id
    pv_metadata.name = pv_system_id
    
    return pv_metadata


def get_pv_statistic(pv_system_id, **kwargs):
    pv_metadata_text = pv_output_api_query(
        service='getstatistic',
        api_params={
            'c': 0,  # consumption and import
            'crdr': 0, # credits / debits
            'sid1': pv_system_id, # SystemID
        }, 
        **kwargs)

    pv_metadata = pd.read_csv(
        StringIO(pv_metadata_text),
        names=[
            'energy_generated_Wh',
            'energy_exported_Wh',
            'average_generation_Wh',
            'minimum_generation_Wh',
            'maximum_generation_Wh',
            'average_efficiency_kWh_per_kW',
            'outputs',
            'actual_date_from',
            'actual_date_to',
            'record_efficiency_kWh_per_kW',
            'record_efficiency_date'
        ],
        parse_dates=['actual_date_from', 'actual_date_to', 'record_efficiency_date']
    ).squeeze()
    pv_metadata['system_id'] = pv_system_id
    pv_metadata.name = pv_system_id
    return pv_metadata
