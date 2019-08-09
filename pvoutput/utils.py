import logging
import sys
import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter


def get_logger(filename='/home/jack/data/pvoutput.org/logs/UK_PV_timeseries.log',
               mode='a',
               level=logging.DEBUG,
               stream_handler=False):
    logger = logging.getLogger('pvoutput')
    logger.setLevel(level)
    logger.handlers = [logging.FileHandler(filename=filename, mode=mode)]
    if stream_handler:
        logger.handlers.append(logging.StreamHandler(sys.stdout))
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    for handler in logger.handlers:
        handler.setFormatter(formatter)

    # Attach urllib3's logger to our logger.
    urllib3_log = logging.getLogger("urllib3")
    urllib3_log.parent = logger
    urllib3_log.propagate = True

    return logger


def _get_session_with_retry() -> requests.Session:
    session = requests.Session()
    max_retry_counts = dict(
        connect=720,  # How many connection-related errors to retry on.
                      # Set high because sometimes the network goes down for a
                      # few hours at a time.
                      # 720 x Retry.MAX_BACKOFF (120 s) = 86,400 s = 24 hrs
        read=5,  # How many times to retry on read errors.
        status=5  # How many times to retry on bad status codes.
    )
    retries = Retry(
        total=max(max_retry_counts.values()),
        backoff_factor=0.5,
        status_forcelist=[500, 502, 503, 504],
        **max_retry_counts
    )
    session.mount('https://', HTTPAdapter(max_retries=retries))
    return session
