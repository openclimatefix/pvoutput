import os
from io import StringIO
import time
import logging
from typing import Dict, Union, Optional
from datetime import datetime, timedelta
import requests
import numpy as np
import pandas as pd

from pvoutput.exceptions import NoStatusFound, RateLimitExceeded
from pvoutput.utils import _get_response, _get_param_from_config_file
from pvoutput.utils import _print_and_log
from pvoutput.consts import ONE_DAY, PV_OUTPUT_DATE_FORMAT, BASE_URL
from pvoutput.consts import CONFIG_FILENAME, RATE_LIMIT_PARAMS_TO_API_HEADERS


_LOG = logging.getLogger('pvoutput')


class PVOutput:
    """
    Attributes:
        api_key
        system_id
        rate_limit_remaining
        rate_limit_total
        rate_limit_reset_time
        data_service_url
    """
    def __init__(self,
                 api_key: str = None,
                 system_id: str = None,
                 config_filename: Optional[str] = CONFIG_FILENAME,
                 data_service_url: Optional[str] = None
                 ):
        """
        Args:
            api_key: Your API key from PVOutput.org.
            system_id: Your system ID from PVOutput.org.  If you don't have a
                PV system then you can register with PVOutput.org and select
                the 'energy consumption only' box.
            config_filename: Optional, the filename of the .yml config file.
            data_service_url: Optional.  If you have subscribed to
                PVOutput.org's data service then add the data service URL here.
                This string must end in '.org'.
        """

        self.api_key = api_key
        self.system_id = system_id
        self.rate_limit_remaining = None
        self.rate_limit_total = None
        self.rate_limit_reset_time = None
        self.data_service_url = data_service_url

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

        # Check for data_service_url
        if self.data_service_url is None:
            try:
                self.data_service_url = _get_param_from_config_file(
                    'data_service_url', config_filename)
            except KeyError:
                pass

        if self.data_service_url is not None:
            if not self.data_service_url.strip('/').endswith('.org'):
                raise ValueError("data_service_url must end in '.org'")

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
                    name,
                    system_AC_capacity_W,
                    address,  # If `include_country` is True then address is
                              # 'country> <postcode>',
                              # else address is '<postcode>'.
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
                'name',
                'system_AC_capacity_W',
                'address',
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
                   date: Union[str, datetime],
                   **kwargs
                   ) -> pd.DataFrame:
        """Get PV system status (e.g. power generation) for one day.

        The returned DataFrame will be empty if the PVOutput API
        returns 'status 400: No status found'.

        Args:
            pv_system_id: int
            date: str in format YYYYMMDD; or datetime
                (localtime of the PV system)

        Returns:
            pd.DataFrame:
                index: datetime (DatetimeIndex, localtime of the PV system)
                columns:  (all np.float64):
                    cumulative_energy_gen_Wh,
                    energy_efficiency_kWh_per_kW,
                    instantaneous_power_gen_W,
                    average_power_gen_W,
                    power_gen_normalised,
                    energy_consumption_Wh,
                    power_demand_W,
                    temperature_C,
                    voltage
        """
        date = date_to_pvoutput_str(date)
        _check_date(date)

        api_params = {
            'd': date,  # date, YYYYMMDD, localtime of the PV system
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

        # See https://pvoutput.org/help.html#api-getstatus but make sure
        # you read the 'History Query' subsection, as a historical query
        # has slightly different return columns compared to a non-historical
        # query!
        columns = [
            'cumulative_energy_gen_Wh',
            'energy_efficiency_kWh_per_kW',
            'instantaneous_power_gen_W',
            'average_power_gen_W',
            'power_gen_normalised',
            'energy_consumption_Wh',
            'power_demand_W',
            'temperature_C',
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

    def get_batch_status(self,
                         pv_system_id: int,
                         date_to: Optional[Union[str, datetime]] = None,
                         max_retries: Optional[int] = 1000,
                         **kwargs
                         ) -> Union[None, pd.DataFrame]:
        """Get batch PV system status (e.g. power generation).

        The returned DataFrame will be empty if the PVOutput API
        returns 'status 400: No status found'.

        Data returned is limited to the last 365 days per request.
        To retrieve older data, use the date_to parameter.

        The PVOutput getbatchstatus API is asynchronous.  When it's first
        called, it replies to say 'accepted'.  This function will then
        wait a minute and call the API again to see if the data is ready.
        Set `max_retries` to 1 if you want to return immediately, even
        if data isn't ready yet (and hence this function will return None)

        Args:
            pv_system_id: int
            date_to: str in format YYYYMMDD; or datetime
                (localtime of the PV system)
            max_retries: int, number of times to retry after receiving
                a '202 Accepted' request.

        Returns:
            pd.DataFrame:
                index: datetime (DatetimeIndex, localtime of the PV system)
                columns:  (all np.float64):
                    cumulative_energy_gen_Wh,
                    instantaneous_power_gen_W,
                    temperature_C,
                    voltage
        """
        api_params = {'sid1': pv_system_id}

        if date_to is not None:
            date_to = date_to_pvoutput_str(date_to)
            _check_date(date_to)
            api_params['dt'] = date_to

        for retry in range(max_retries):
            try:
                pv_system_status_text = self._api_query(
                    service='getbatchstatus', api_params=api_params,
                    use_data_service=True, **kwargs)
            except NoStatusFound:
                _LOG.info(
                    'system_id %d: No status found for date_to %s',
                    pv_system_id, date_to)
                pv_system_status_text = ""
                break

            if 'Accepted 202' in pv_system_status_text:
                if retry == 0:
                    _print_and_log('Request accepted.')
                if retry < max_retries - 1:
                    _print_and_log('Sleeping for 1 minute.')
                    time.sleep(60)
            else:
                break
        else:
            return

        return _process_batch_status(pv_system_status_text)

    def get_metadata(self, pv_system_id: int, **kwargs) -> pd.Series:
        """Get metadata for a single PV system.

        Args:
            pv_system_id: int

        Returns:
            pd.Series.  Index is:
                name,
                system_AC_capacity_W,
                address,
                num_panels,
                panel_capacity_W_each,
                panel_brand,
                num_inverters,
                inverter_capacity_W,
                inverter_brand,
                orientation,
                array_tilt_degrees,
                shade,
                install_date,
                latitude,
                longitude,
                status_interval_minutes,
                secondary_num_panels,
                secondary_panel_capacity_W_each,
                secondary_orientation,
                secondary_array_tilt_degrees
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
                'name',
                'system_AC_capacity_W',
                'address',
                'num_panels',
                'panel_capacity_W_each',
                'panel_brand',
                'num_inverters',
                'inverter_capacity_W',
                'inverter_brand',
                'orientation',
                'array_tilt_degrees',
                'shade',
                'install_date',
                'latitude',
                'longitude',
                'status_interval_minutes',
                'secondary_num_panels',
                'secondary_panel_capacity_W_each',
                'secondary_orientation',
                'secondary_array_tilt_degrees'
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
            pd.Series:
                total_energy_gen_Wh,
                energy_exported_Wh,
                average_daily_energy_gen_Wh,
                minimum_daily_energy_gen_Wh,
                maximum_daily_energy_gen_Wh,
                average_efficiency_kWh_per_kW,
                num_outputs,
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
                'total_energy_gen_Wh',
                'energy_exported_Wh',
                'average_daily_energy_gen_Wh',
                'minimum_daily_energy_gen_Wh',
                'maximum_daily_energy_gen_Wh',
                'average_efficiency_kWh_per_kW',
                'num_outputs',
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

    def _api_query(self,
                   service: str,
                   api_params: Dict,
                   wait_if_rate_limit_exceeded: bool = False,
                   use_data_service: bool = False
                   ) -> str:
        """Send API request to PVOutput.org and return content text.

        Args:
            service: string, e.g. 'search' or 'getstatus'
            api_params: dict
            wait_if_rate_limit_exceeded: bool
            use_data_service: bool

        Raises:
            NoStatusFound
            RateLimitExceeded
        """
        get_response_func = (
            self._get_data_service_response if use_data_service else
            self._get_api_response)

        try:
            response = get_response_func(service, api_params)
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
            _print_and_log(msg)
            if wait_if_rate_limit_exceeded:
                self.wait_for_rate_limit_reset()
                return self._api_query(
                    service, api_params, wait_if_rate_limit_exceeded=False)

            raise RateLimitExceeded(response, msg)

    def _get_api_response(self,
                          service: str,
                          api_params: Dict
                          ) -> requests.Response:
        """
        Args:
            service: string, e.g. 'search', 'getstatus'
            api_params: dict
        """
        self._check_api_params()
        # Create request headers
        headers = {
            'X-Rate-Limit': '1',
            'X-Pvoutput-Apikey': self.api_key,
            'X-Pvoutput-SystemId': self.system_id}

        api_url = os.path.join(
            BASE_URL, 'service/r2/{}.jsp'.format(service))

        return _get_response(api_url, api_params, headers)

    def _get_data_service_response(self,
                                   service: str,
                                   api_params: Dict
                                   ) -> requests.Response:
        """
        Args:
            service: string, e.g. 'getbatchstatus'
            api_params: dict
        """
        self._check_api_params()
        if self.data_service_url is None:
            raise ValueError(
                'data_service_url must be set to use the data service!')

        headers = {'X-Rate-Limit': '1'}
        api_params = api_params.copy()
        api_params['key'] = self.api_key
        api_params['sid'] = self.system_id

        api_url = os.path.join(
            self.data_service_url, 'service/r2/{}.jsp'.format(service))

        return _get_response(api_url, api_params, headers)

    def _check_api_params(self):
        # Check we have relevant login details:
        for param_name in ['api_key', 'system_id']:
            if getattr(self, param_name) is None:
                raise ValueError(
                    'Please set the {} parameter.'.format(param_name))

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
            response: from _get_api_response()

        Returns:
            content of the response.

        Raises:
            UnicodeDecodeError
            NoStatusFound
            RateLimitExceeded
        """
        if response.status_code == 400:
            raise NoStatusFound(response=response)

        if response.status_code != 403:
            try:
                response.raise_for_status()
            except Exception as e:
                msg = (
                    'Bad status code! Response content = {}. Exception = {}'
                    .format(response.content, e))
                _LOG.exception(msg)
                raise e.__class__(msg)

        self._set_rate_limit_params(response.headers)

        # Did we overshoot our quota?
        if response.status_code == 403 and self.rate_limit_remaining <= 0:
            raise RateLimitExceeded(response=response)

        try:
            content = response.content.decode('latin1').strip()
        except Exception as e:
            msg = "Error decoding this string: {}\n{}".format(
                response.content, e)
            _LOG.exception(msg)
            raise

        # If we get to here then the content is valid :)
        return content

    def wait_for_rate_limit_reset(self):
        utc_now = pd.Timestamp.utcnow()
        timedelta_to_wait = self.rate_limit_reset_time - utc_now
        timedelta_to_wait += timedelta(minutes=3)  # Just for safety
        secs_to_wait = timedelta_to_wait.total_seconds()
        retry_time_utc = utc_now + timedelta_to_wait
        _print_and_log('Waiting {:.0f} seconds.  Will retry at {}'.format(
            secs_to_wait, retry_time_utc))
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


def _process_batch_status(pv_system_status_text):
    # See https://pvoutput.org/help.html#dataservice-getbatchstatus

    # PVOutput uses a non-standard format for the data.  The text
    # needs some processing before it can be read as a CSV.
    processed_lines = []
    for line in pv_system_status_text.split('\n'):
        line_sections = line.split(';')
        date = line_sections[0]
        time_and_data = line_sections[1:]
        processed_line = [
            '{date},{payload}'.format(date=date, payload=payload)
            for payload in time_and_data]
        processed_lines.extend(processed_line)

    if processed_lines:
        first_line = processed_lines[0]
        num_cols = len(first_line.split(','))
        if num_cols >= 8:
            raise NotImplementedError(
                'Handling of consumption data is not implemented!')

    processed_text = '\n'.join(processed_lines)
    del processed_lines

    columns = [
        'cumulative_energy_gen_Wh',
        'instantaneous_power_gen_W',
        'temperature_C',
        'voltage']

    pv_system_status = pd.read_csv(
        StringIO(processed_text),
        names=['date', 'time'] + columns,
        parse_dates={'datetime': ['date', 'time']},
        index_col=['datetime'],
        dtype={col: np.float64 for col in columns}
    ).sort_index()

    return pv_system_status
