from datetime import date, datetime
from io import StringIO

import numpy as np
import pandas as pd
import pytest

from pvoutput import pvoutput


def test_init():
    _ = pvoutput.PVOutput(api_key="fake", system_id="fake")


def test_rate_limit():
    pv = pvoutput.PVOutput(api_key="fake", system_id="fake")

    # set a fake reset time
    pv.rate_limit_reset_time = pd.Timestamp.utcnow() + pd.Timedelta(minutes=30)

    # get the number of seconds we need to wait
    seconds_to_wait = pv.wait_for_rate_limit_reset(do_sleeping=False)

    # 30 mins, + 3 mins for safety
    assert np.round(seconds_to_wait) == 30 * 60 + (60 * 3)


@pytest.mark.skip("Currently not working in CI")
def test_get_status():
    pv = pvoutput.PVOutput()
    pv.get_status(
        pv_system_id=10033,
        date=datetime(2022, 3, 1, 12),
        use_data_service=True,
        timezone="Europe/London",
    )


@pytest.mark.skip("Currently not working in CI")
def test_multiple_get_status():
    pv = pvoutput.PVOutput()
    status_df = pv.get_system_status(
        pv_system_ids=[10033, 10020],
        date=datetime(2022, 3, 15),
        use_data_service=True,
        timezone="Europe/London",
    )

    assert len(status_df) > 0


def test_convert_consecutive_dates_to_date_ranges():
    dr1 = pd.date_range("2018-01-01", "2018-02-01", freq="D").tolist()
    dr2 = pd.date_range("2018-02-05", "2018-02-10", freq="D").tolist()
    missing_dates = dr1 + dr2
    date_ranges = pvoutput._convert_consecutive_dates_to_date_ranges(missing_dates)
    columns = ["missing_start_date_PV_localtime", "missing_end_date_PV_localtime"]
    pd.testing.assert_frame_equal(
        date_ranges[columns],
        pd.DataFrame(
            [
                [dr1[0], dr1[-1]],
                [dr2[0], dr2[-1]],
            ],
            columns=columns,
        ),
    )


def test_date_to_pvoutput_str():
    VALID_DATE_STR = "20190101"
    assert pvoutput.date_to_pvoutput_str(VALID_DATE_STR) == VALID_DATE_STR
    ts = pd.Timestamp(VALID_DATE_STR)
    assert pvoutput.date_to_pvoutput_str(ts) == VALID_DATE_STR


def test_check_date():
    assert pvoutput._check_date("20190101") is None
    with pytest.raises(ValueError):
        pvoutput._check_date("2010")
    with pytest.raises(ValueError):
        pvoutput._check_date("2010-01-02")


def test_check_pv_system_status():
    def _make_timeseries(start, end):
        index = pd.date_range(start, end, freq="5T")
        n = len(index)
        timeseries = pd.DataFrame(np.zeros(n), index=index)
        return timeseries

    DATE = date(2019, 1, 1)
    good_timeseries = _make_timeseries("2019-01-01 00:00", "2019-01-02 00:00")
    pvoutput.check_pv_system_status(good_timeseries, DATE)

    bad_timeseries = _make_timeseries("2019-01-01 00:00", "2019-01-03 00:00")
    with pytest.raises(ValueError):
        pvoutput.check_pv_system_status(bad_timeseries, DATE)

    bad_timeseries2 = _make_timeseries("2019-01-02 00:00", "2019-01-03 00:00")
    with pytest.raises(ValueError):
        pvoutput.check_pv_system_status(bad_timeseries2, DATE)


def test_read_csv_calls_do_not_use_deprecated_parse_dates(monkeypatch):
    pv = pvoutput.PVOutput(api_key="fake", system_id="fake")

    original_read_csv = pd.read_csv
    parse_dates_values = []

    def wrapped_read_csv(*args, **kwargs):
        if "parse_dates" in kwargs:
            parse_dates_values.append(kwargs["parse_dates"])
        return original_read_csv(*args, **kwargs)

    monkeypatch.setattr(pd, "read_csv", wrapped_read_csv)

    api_responses = {
        "getstatus": "20220301,08:00,2,3,4,5,6,7,8;20220301,07:45,1,2,3,4,5,6,7",
        "getsystem": (
            "System Name,5000,Address,10,500,PanelBrand,1,5000,InverterBrand,180,30,None,"
            "2020-01-01,-0.1,51.5,5,0,0,0,0"
        ),
        "getstatistic": "100,10,5,1,9,2,30,2022-01-01,2022-01-31,3,2022-01-15",
    }

    def fake_api_query(service, api_params, **kwargs):
        return api_responses[service]

    monkeypatch.setattr(pv, "_api_query", fake_api_query)

    pv.get_status(pv_system_id=1, date="20220301", historic=False)
    pv.get_metadata(pv_system_id=1)
    pv.get_statistic(pv_system_id=1, date_from="20220101", date_to="20220131")

    assert not parse_dates_values


def test_get_status_datetime_index_contract(monkeypatch):
    pv = pvoutput.PVOutput(api_key="fake", system_id="fake")

    def fake_api_query(service, api_params, **kwargs):
        assert service == "getstatus"
        return "20220301,08:00,2,3,4,5,6,7,8;20220301,07:45,1,2,3,4,5,6,7"

    monkeypatch.setattr(pv, "_api_query", fake_api_query)

    result = pv.get_status(pv_system_id=1, date="20220301", historic=False)

    assert isinstance(result.index, pd.DatetimeIndex)
    assert result.index.name == "datetime"
    assert result.index.is_monotonic_increasing
    assert result.index[0] == pd.Timestamp("2022-03-01 07:45:00")
    assert result.index[-1] == pd.Timestamp("2022-03-01 08:00:00")
    assert "date" not in result.columns
    assert "time" not in result.columns
    assert "datetime" not in result.columns


def test_get_status_timezone_conversion_contract(monkeypatch):
    pv = pvoutput.PVOutput(api_key="fake", system_id="fake")

    def fake_api_query(service, api_params, **kwargs):
        assert service == "getstatus"
        return "20220101,07:45,1,2,3,4,5,6,7"

    monkeypatch.setattr(pv, "_api_query", fake_api_query)

    result = pv.get_status(
        pv_system_id=1,
        date="20220101",
        historic=False,
        timezone="Europe/London",
    )

    assert isinstance(result.index, pd.DatetimeIndex)
    assert result.index.name == "datetime"
    assert result.index.tz is not None
    assert result.index[0].utcoffset() == pd.Timedelta(0)


def test_get_metadata_install_date_type_contract(monkeypatch):
    pv = pvoutput.PVOutput(api_key="fake", system_id="fake")

    def fake_api_query(service, api_params, **kwargs):
        assert service == "getsystem"
        return (
            "System Name,5000,Address,10,500,PanelBrand,1,5000,InverterBrand,180,30,None,"
            "2020-01-01,-0.1,51.5,5,0,0,0,0"
        )

    monkeypatch.setattr(pv, "_api_query", fake_api_query)

    result = pv.get_metadata(pv_system_id=123)

    assert isinstance(result["install_date"], pd.Timestamp)
    assert result["install_date"] == pd.Timestamp("2020-01-01")


def test_get_statistic_date_columns_type_contract(monkeypatch):
    pv = pvoutput.PVOutput(api_key="fake", system_id="fake")

    def fake_api_query(service, api_params, **kwargs):
        assert service == "getstatistic"
        return "100,10,5,1,9,2,30,2022-01-01,2022-01-31,3,2022-01-15"

    monkeypatch.setattr(pv, "_api_query", fake_api_query)

    result = pv.get_statistic(pv_system_id=1, date_from="20220101", date_to="20220131")

    assert pd.api.types.is_datetime64_any_dtype(result["actual_date_from"])
    assert pd.api.types.is_datetime64_any_dtype(result["actual_date_to"])
    assert pd.api.types.is_datetime64_any_dtype(result["record_efficiency_date"])
    assert result.loc[1, "actual_date_from"] == pd.Timestamp("2022-01-01")
    assert result.loc[1, "actual_date_to"] == pd.Timestamp("2022-01-31")
    assert result.loc[1, "record_efficiency_date"] == pd.Timestamp("2022-01-15")
