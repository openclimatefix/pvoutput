import os
from datetime import timedelta


ONE_DAY = timedelta(days=1)

PV_OUTPUT_DATE_FORMAT = "%Y%m%d"
CONFIG_FILENAME = os.path.expanduser("~/.pvoutput.yml")
RATE_LIMIT_PARAMS_TO_API_HEADERS = {
    'rate_limit_remaining': 'X-Rate-Limit-Remaining',
    'rate_limit_total': 'X-Rate-Limit-Limit',
    'rate_limit_reset_time': 'X-Rate-Limit-Reset'}
