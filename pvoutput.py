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
import os
import re
from datetime import datetime, timedelta
import requests
import pandas as pd

SECONDS_PER_DAY = 60 * 60 * 24


class BadStatusCode(Exception):
    def __init__(self, message='', status_code, response_content, response_headers):
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
    

def pv_output_api_query(service, api_params, retries=5):
    """
    Args:
        service: string, e.g. 'search', 'getstatus'
        api_params: dict
        retries: int
    Raises:
        MaxRetryError
        BadStatusCode
    """   
    headers = {
        'X-Pvoutput-Apikey': os.environ['PVOUTPUT_APIKEY'],
        'X-Pvoutput-SystemId': os.environ['PVOUTPUT_SYSTEMID'],
        'X-Rate-Limit': '1'}

    api_base_url = 'https://pvoutput.org/service/r2/{}.jsp'.format(service)
    api_params_str = '&'.join(['{}={}'.format(key, value) for key, value in api_params.items()])
    api_url = '{}?{}'.format(api_base_url, api_params_str)
    response = requests.get(api_url, headers=headers)

    try:
        content = response.content.decode('latin1').strip()
    except UnicodeDecodeError as e:
        print("Error decoding this string: {}\n{}".format(content, e))
        raise

    # Did we overshoot our quota?
    if response.status_code == 403 and response.headers['X-Rate-Limit-Remaining'] == '0':
        if retries == 0:
            raise MaxRetryError(
                status_code=response.status_code, 
                response_content=content,
                headers=response.headers)
        # Wait until rate limit is reset
        rate_limit_reset_datetime = datetime.utcfromtimestamp(
            int(response.headers['X-Rate-Limit-Reset']))
        timedelta_to_wait = rate_limit_reset_datetime - datetime.utcnow()
        timedelta_to_wait += timedelta(minutes=3) # Just for safety
        retry_time = datetime.now() + timedelta_to_wait
        secs_to_wait = timedelta.total_seconds()
        print('Rate limit exceeded!  Waiting {:d} seconds.  Will retry at {}.'.format(
            secs_to_wait, retry_time.strftime('%Y-%M-%d %H:%m:%S')))
        print('Retries remaining =', retries)
        time.sleep(secs_to_wait)
        return pv_output_api_query(service, api_params, retries=retries-1)

    if response.status_code != 200:
        if retries == 0:
            raise BadStatusCode(
                status_code=response.status_code, 
                response_content=content,
                headers=response.headers)
        print("Received bad status code:", response.status_code, content)
        print("Retrying...")
        time.sleep(1)
        return pv_output_api_query(service, api_params, retries=retries-1)
    else:
        return content
        

def pv_system_search(query, lat_lon):
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
        })
    
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


def get_pv_system_status(pv_system_id, date):
    """
    Args:
        pv_system_id: int
        date: str, YYYYMMDD
    """
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
        })
    
    pv_system_status = pd.read_csv(
        StringIO(pv_system_status_text),
        lineterminator=';',
        names=[
            'date',
            'time',
            'energy_generation_watt_hours',
            'energy_efficiency_kWh_per_kW',
            'inst_power_watt',
            'average_power_watt',
            'normalised_output',
            'energy_consumption_watt_hours',
            'power_consumption_watts',
            'temperature_celsius',
            'voltage'
        ],
        parse_dates={'datetime': ['date', 'time']},
        index_col=['datetime']
    )
    
    return pv_system_status


def get_pv_metadata(pv_system_id):
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
        })
    
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