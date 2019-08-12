import os
from datetime import timedelta


BASE_URL = 'https://pvoutput.org'
MAP_URL = os.path.join(BASE_URL, 'map.jsp')

# Country codes used by PVOutput.org on, for example,
# https://pvoutput.org/map.jsp.  Taken from
# https://pvoutput.org/help.html#api-addsystem.
# TODO: Add all country codes, see issue #16.
PV_OUTPUT_COUNTRY_CODES = {
    'Australia': 1,
    'Afghanistan': 2,
    'Akrotiri': 3,
    'Albania': 4,
    'Algeria': 5,
    'American Samoa': 6,
    'Andorra': 7,
    'Angola': 8,
    'Anguilla': 9,
    'Antarctica': 10,
    'Antigua and Barbuda': 11,
    'Arctic Ocean': 12,
    'Argentina': 13,
    'Armenia': 14,
    'Aruba': 15,
    'Ashmore and Cartier Islands': 16,
    'Atlantic Ocean': 17,
    'Austria': 18,
    'Azerbaijan': 19,
    'Bahamas': 20,
    'Netherlands': 165,
    'United Kingdom': 243,
    'United States': 244,
    'Zimbabwe': 257  # Last country code.
}

PV_OUTPUT_MAP_COLUMN_NAMES = {
    'timeseries_duration': 'c',
    'average_generation_per_day': 'avg',
    'efficiency': 'gss',
    'power_generation': 'atg',
    'capacity': 'ss',
    'address': 'o',
    'name': 'sn'
}


ONE_DAY = timedelta(days=1)

PV_OUTPUT_DATE_FORMAT = "%Y%m%d"
CONFIG_FILENAME = os.path.expanduser("~/.pvoutput.yml")
RATE_LIMIT_PARAMS_TO_API_HEADERS = {
    'rate_limit_remaining': 'X-Rate-Limit-Remaining',
    'rate_limit_total': 'X-Rate-Limit-Limit',
    'rate_limit_reset_time': 'X-Rate-Limit-Reset'}

