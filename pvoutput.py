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
    def __init__(self, message='', status_code=None, response_content=None, response_headers=None):
        self.status_code = status_code
        self.response_content = response_content
        self.response_headers = response_headers
        super(BadStatusCode, self).__init__(message)
        
    def __str__(self):
        string = super(BadStatusCode, self).__str__()
        string += " Bad status code returned: {}\n".format(self.status_code)
        string += " Response content: {}\n".format(self.response_content)
        string += " Response headers: {}".format(self.response_headers)
        return string


class MaxRetryError(BadStatusCode):
    pass
    

class NoStatusFound(BadStatusCode):
    pass
    

def pv_output_api_query(service, api_params, retries=150, seconds_between_retries=300):
    """
    Args:
        service: string, e.g. 'search', 'getstatus'
        api_params: dict
        retries: int
    Raises:
        MaxRetryError
        BadStatusCode
        NoStatusFound
    """   
    headers = {
        'X-Pvoutput-Apikey': os.environ['PVOUTPUT_APIKEY'],
        'X-Pvoutput-SystemId': os.environ['PVOUTPUT_SYSTEMID'],
        'X-Rate-Limit': '1'}

    api_base_url = 'https://pvoutput.org/service/r2/{}.jsp'.format(service)
    api_params_str = '&'.join(['{}={}'.format(key, value) for key, value in api_params.items()])
    api_url = '{}?{}'.format(api_base_url, api_params_str)
    logger.debug('api_url=%s; headers=%s; service=%s; api_params=%s; retries=%d',
                api_url, headers, service, api_params, retries)
    
    try:
        response = requests.get(api_url, headers=headers)
    except Exception as e:
        msg = "request.get() failed"
        print(msg)
        logger.exception(msg)
        if retries == 0:
            raise
        else:
            time.sleep(seconds_between_retries)
            return pv_output_api_query(service, api_params, retries=retries-1, 
                                       seconds_between_retries=seconds_between_retries)

    try:
        content = response.content.decode('latin1').strip()
    except UnicodeDecodeError as e:
        msg = "Error decoding this string: {}\n{}".format(content, e)
        print(msg)
        logger.exception(msg)
        raise

    if response.status_code == 400:
        # No point retrying if no status was found
        raise NoStatusFound(
                status_code=response.status_code, 
                response_content=content,
                response_headers=response.headers)
        
    # Did we overshoot our quota?
    if response.status_code == 403 and int(response.headers['X-Rate-Limit-Remaining']) <= 0:
        if retries == 0:
            raise MaxRetryError(
                status_code=response.status_code, 
                response_content=content,
                response_headers=response.headers)
        # Wait until rate limit is reset
        rate_limit_reset_datetime = datetime.utcfromtimestamp(
            int(response.headers['X-Rate-Limit-Reset']))
        timedelta_to_wait = rate_limit_reset_datetime - datetime.utcnow()
        timedelta_to_wait += timedelta(minutes=3) # Just for safety
        retry_time = datetime.now() + timedelta_to_wait
        secs_to_wait = timedelta_to_wait.total_seconds()
        msg = 'Rate limit exceeded!  Waiting {:.0f} seconds.  Will retry at {}.\n'.format(
            secs_to_wait, retry_time.strftime('%Y-%m-%d %H:%M:%S'))
        msg += 'Retries remaining = {}'.format(retries)
        print(msg)
        logger.warning(msg)
        time.sleep(secs_to_wait)
        return pv_output_api_query(service, api_params, retries=retries-1, seconds_between_retries=seconds_between_retries)

    if response.status_code == 200:  # Good status code.  Yay!
        return content
    else:
        if retries == 0:
            raise BadStatusCode(
                status_code=response.status_code, 
                response_content=content,
                response_headers=response.headers)
        msg = "Received bad status code: {} {}\nRetrying...".format(response.status_code, content)
        print(msg)
        logger.warning(msg)
        time.sleep(seconds_between_retries)
        return pv_output_api_query(service, api_params, retries=retries-1, seconds_between_retries=seconds_between_retries)
        


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
            'd': date, # date, YYYYMMDD
            'h': 1,  # History; we want historical data
            'limit': SECONDS_PER_DAY,  # API docs say limit is 288 (number of 5-min periods per day),
                                       # but let's try to get number of secs per day just in case 
                                       # some PV systems have 1-second updates.
            'ext': 0, # extended data; we don't want extended data because it's not clear how to parse it.
            'sid1': pv_system_id # SystemID
        }, **kwargs)
    
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
                  raise ValueError('A date in the index is outside the expected range. Date from index={}, requested_date={}'.format(d, requested_date_str))


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