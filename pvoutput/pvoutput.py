from io import StringIO
import time
import logging
from typing import Dict, Union, Optional
from datetime import datetime, timedelta
import requests
import numpy as np
import pandas as pd

from .exceptions import NoStatusFound, RateLimitExceeded
from .utils import _get_session_with_retry, _get_param_from_config_file
from .consts import ONE_DAY, PV_OUTPUT_DATE_FORMAT
from .consts import CONFIG_FILENAME, RATE_LIMIT_PARAMS_TO_API_HEADERS


_LOG = logging.getLogger('pvoutput')


class PVOutput:
    """
    Attributes:
        api_key
        system_id
        rate_limit_remaining
        rate_limit_total
        rate_limit_reset_time
    """
    def __init__(self,
                 api_key: str = None,
                 system_id: str = None,
                 config_filename: str = CONFIG_FILENAME):

        self.api_key = api_key
        self.system_id = system_id
        self.rate_limit_remaining = None
        self.rate_limit_total = None
        self.rate_limit_reset_time = None

        # Set from config file if None
        for param_name in ['api_key', 'system_id']:
            if getattr(self, param_name) is None:
                try:
                    param_value_from_config = _get_param_from_config_file(
                        param_name, config_filename)
                except Exception as e:
                    msg = (
                        'Error loading configuration parameter {param_name}'
                        ' from config file {filename}.  Either pass'
                        ' {param_name} into PVOutput constructor, or create'
                        ' config file {filename}.  {exception}'.format(
                            param_name=param_name,
                            filename=CONFIG_FILENAME,
                            exception=e))
                    print(msg)
                    _LOG.exception(msg)
                    raise
                setattr(self, param_name, param_value_from_config)
            # Convert to strings
            setattr(self, param_name, str(getattr(self, param_name)))

    def search(self,
               query: str,
               lat: Optional[float] = None,
               lon: Optional[float] = None,
               include_country: bool = True,
               **kwargs
               ) -> pd.DataFrame:
        """Search for PV systems.

        Some quirks of the PVOutput.org API:
            - The maximum number of results returned by PVOutput.org is 30.
                If the number of returned results is 30, then there is no
                indication of whether there are exactly 30 search results,
                or if there are more than 30.  Also, there is no way to
                request additional 'pages' of search results.
            - The maximum search radius is 25km

        Args:
            query: string, see https://pvoutput.org/help.html#search
                e.g. '5km'.
            lat: float, e.g. 52.0668589
            lon: float, e.g. -1.3484038
            include_country: bool, whether or not to include the country name
                with the returned postcode.

        Returns:
            pd.DataFrame, one row per search results.  Index is PV system ID.
                Columns:
                    system_name,
                    system_size_watts,
                    postcode,  # including the country
                    orientation,
                    num_outputs,
                    last_output,
                    panel,
                    inverter,
                    distance_km,
                    latitude,
                    longitude
        """
        api_params = {'q': query, 'country': int(include_country)}

        if lat is not None and lon is not None:
            api_params['ll'] = '{:f},{:f}'.format(lat, lon)

        pv_systems_text = self._api_query(
            service='search', api_params=api_params, **kwargs)

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

    def get_status(self,
                   pv_system_id: int,
                   date: str,
                   **kwargs
                   ) -> pd.DataFrame:
        """Get PV system status (e.g. power generation) for one day.

        The returned DataFrame will be empty if the PVOutput API
        returns 'status 400: No status found'.

        Args:
            pv_system_id: int
            date: str, YYYYMMDD

        Returns:
            pd.DataFrame:
                index: datetime (DatetimeIndex, localtime of the PV system)
                columns:  (all np.float64):
                    energy_generation_watt_hours,
                    energy_efficiency_kWh_per_kW,
                    inst_power_watt,
                    average_power_watt,
                    normalised_output,
                    energy_consumption_watt_hours,
                    power_consumption_watts,
                    temperature_celsius,
                    voltage
        """
        date = date_to_pvoutput_str(date)
        _check_date(date)

        api_params = {
            'd': date,  # date, YYYYMMDD.
            'h': 1,  # We want historical data.
            'limit': 288,  # API limit is 288 (num of 5-min periods per day).
            'ext': 0,  # Extended data; we don't want extended data.
            'sid1': pv_system_id  # SystemID.
        }

        try:
            pv_system_status_text = self._api_query(
                service='getstatus', api_params=api_params, **kwargs)
        except NoStatusFound:
            _LOG.info(
                'system_id %d: No status found for date %s',
                pv_system_id, date)
            pv_system_status_text = ""

        columns = [
            'energy_generation_watt_hours',
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

    def get_metadata(self, pv_system_id: int, **kwargs) -> pd.Series:
        """Get metadata for a single PV system.

        Args:
            pv_system_id: int

        Returns:
            pd.Series.  Index is:
                system_name,
                system_id,
                system_size_watts,
                postcode,
                number_of_panels,
                panel_power_watts,
                panel_brand,
                num_inverters,
                inverter_power_watts,
                inverter_brand,
                orientation,
                array_tilt_degrees,
                shade,
                install_date,
                latitude,
                longitude,
                status_interval_minutes,
                number_of_panels_secondary,
                panel_power_watts_secondary,
                orientation_secondary,
                array_tilt_degrees_secondary
        """
        pv_metadata_text = self._api_query(
            service='getsystem',
            api_params={
                'array2': 1,  # Provide data about secondary array, if present.
                'tariffs': 0,
                'teams': 0,
                'est': 0,
                'donations': 0,
                'sid1': pv_system_id,  # SystemID
                'ext': 0,  # Include extended data?
            },
            **kwargs)

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

    def get_statistic(self, pv_system_id: int, **kwargs) -> pd.Series:
        """Get summary stats for a single PV system.

        Args:
            pv_system_id: int

        Returns:
            pd.Series with index:
                energy_generated_Wh,
                energy_exported_Wh,
                average_generation_Wh,
                minimum_generation_Wh,
                maximum_generation_Wh,
                average_efficiency_kWh_per_kW,
                outputs,  # Total number of data outputs recorded by PVOutput.
                actual_date_from,
                actual_date_to,
                record_efficiency_kWh_per_kW,
                record_efficiency_date,
                system_id
        """

        pv_metadata_text = self._api_query(
            service='getstatistic',
            api_params={
                'c': 0,  # consumption and import
                'crdr': 0,  # credits / debits
                'sid1': pv_system_id,  # SystemID
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
            parse_dates=[
                'actual_date_from',
                'actual_date_to',
                'record_efficiency_date'
            ]
        ).squeeze()
        pv_metadata['system_id'] = pv_system_id
        pv_metadata.name = pv_system_id
        return pv_metadata

    def _get_api_reponse(self,
                         service: str,
                         api_params: Dict
                         ) -> requests.Response:
        """
        Args:
            service: string, e.g. 'search', 'getstatus'
            api_params: dict
        """
        # Check we have relevant login details:
        for param_name in ['api_key', 'system_id']:
            if getattr(self, param_name) is None:
                raise ValueError(
                    'Please set the {} parameter.'.format(param_name))

        # Create request headers
        headers = {
            'X-Pvoutput-Apikey': self.api_key,
            'X-Pvoutput-SystemId': self.system_id,
            'X-Rate-Limit': '1'}

        # Create request URL
        api_base_url = 'https://pvoutput.org/service/r2/{}.jsp'.format(service)
        api_params_str = '&'.join(
            ['{}={}'.format(key, value) for key, value in api_params.items()])
        api_url = '{}?{}'.format(api_base_url, api_params_str)

        session = _get_session_with_retry()
        response = session.get(api_url, headers=headers)

        _LOG.debug(
            'response: status_code=%d; headers=%s',
            response.status_code, response.headers)
        self._set_rate_limit_params(response.headers)
        return response

    def _set_rate_limit_params(self, headers):
        for param_name, header_key in RATE_LIMIT_PARAMS_TO_API_HEADERS.items():
            header_value = int(headers[header_key])
            setattr(self, param_name, header_value)

        self.rate_limit_reset_time = pd.Timestamp.utcfromtimestamp(
            self.rate_limit_reset_time)
        self.rate_limit_reset_time = self.rate_limit_reset_time.tz_localize(
            'utc')

        _LOG.debug('%s', self.rate_limit_info())

    def rate_limit_info(self) -> Dict:
        info = {}
        for param_name in RATE_LIMIT_PARAMS_TO_API_HEADERS:
            info[param_name] = getattr(self, param_name)
        return info

    def _process_api_response(self, response: requests.Response) -> str:
        """Turns an API response into text.

        Args:
            response: from _get_api_reponse()

        Returns:
            content of the response.

        Raises:
            UnicodeDecodeError
            NoStatusFound
            RateLimitExceeded
        """
        # Did we overshoot our quota?
        if response.status_code == 403 and self.rate_limit_remaining <= 0:
            raise RateLimitExceeded(response=response)

        if response.status_code == 400:
            raise NoStatusFound(response=response)

        response.raise_for_status()

        try:
            content = response.content.decode('latin1').strip()
        except Exception as e:
            msg = "Error decoding this string: {}\n{}".format(
                response.content, e)
            _LOG.exception(msg)
            raise

        # If we get to here then the content is valid :)
        return content

    def _api_query(self,
                   service: str,
                   api_params: Dict,
                   wait_if_rate_limit_exceeded: bool = False
                   ) -> str:
        """Send API request to PVOutput.org and return content text.

        Args:
            service: string, e.g. 'search' or 'getstatus'
            api_params: dict
            wait_if_rate_limit_exceeded: bool

        Raises:
            NoStatusFound
            RateLimitExceeded
        """
        try:
            response = self._get_api_reponse(service, api_params)
        except Exception as e:
            _LOG.exception(e)
            raise

        try:
            return self._process_api_response(response)
        except RateLimitExceeded:
            msg = (
                "PVOutput.org API rate limit exceeded!"
                "  Rate limit will be reset at {}".format(
                    self.rate_limit_reset_time))
            _LOG.info(msg)
            print(msg)
            if wait_if_rate_limit_exceeded:
                self.wait_for_rate_limit_reset()
                return self._api_query(
                    service, api_params, wait_if_rate_limit_exceeded=False)

            raise

    def wait_for_rate_limit_reset(self):
        utc_now = pd.Timestamp.utcnow()
        timedelta_to_wait = self.rate_limit_reset_time - utc_now
        timedelta_to_wait += timedelta(minutes=3)  # Just for safety
        secs_to_wait = timedelta_to_wait.total_seconds()
        retry_time_utc = utc_now + timedelta_to_wait
        msg = 'Waiting {:.0f} seconds.  Will retry at {}'.format(
            secs_to_wait, retry_time_utc)
        _LOG.info(msg)
        print(msg)
        time.sleep(secs_to_wait)


def date_to_pvoutput_str(date: Union[str, datetime]) -> str:
    """Convert datetime to date string for PVOutput.org in YYYYMMDD format."""
    if isinstance(date, str):
        return date
    return date.strftime(PV_OUTPUT_DATE_FORMAT)


def _check_date(date: str):
    """Check that date string conforms to YYYYMMDD format,
    and that the date isn't in the future.

    Raises:
        ValueError if the date is 'bad'.
    """
    dt = datetime.strptime(date, PV_OUTPUT_DATE_FORMAT)
    if dt > datetime.now():
        raise ValueError(
            'date should not be in the future.  Got {}.  Current date is {}.'
            .format(date, datetime.now()))


def check_pv_system_status(pv_system_status: pd.DataFrame,
                           requested_date_str: str):
    """Checks the DataFrame returned by get_pv_system_status.

    Args:
        pv_system_status: DataFrame returned by get_pv_system_status
        requested_date_str: Date string in YYYYMMDD format.

    Raises:
        ValueError if the DataFrame is incorrect.
    """
    if not isinstance(pv_system_status, pd.DataFrame):
        raise ValueError('pv_system_status must be a dataframe')
    requested_date = datetime.strptime(requested_date_str, "%Y%m%d").date()
    if not pv_system_status.empty:
        index = pv_system_status.index
        for d in [index[0], index[-1]]:
            if not requested_date <= d.date() <= requested_date + ONE_DAY:
                raise ValueError(
                    'A date in the index is outside the expected range.'
                    ' Date from index={}, requested_date={}'
                    .format(d, requested_date_str))
