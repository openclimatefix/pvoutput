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
