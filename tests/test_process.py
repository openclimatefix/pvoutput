from datetime import date
from io import StringIO

import numpy as np
import pandas as pd
import pytest

from pvoutput.prcoess import process_batch_status, process_system_status


def test_process_system_status():
    pv_system_status_text = "1234;07:45,21,255,1,2;"
    one_status = process_system_status(
        pv_system_status_text=pv_system_status_text, date=date(2022, 1, 1)
    )
    assert len(one_status) == 1


def test_process_system_status_2():
    # note that the second entry has a missing data value
    pv_system_status_text = "1234;07:45,21,255,1,5;" "07:50,22,257,2;" "07:55,23,256,3,4"

    one_status = process_system_status(
        pv_system_status_text=pv_system_status_text, date=date(2022, 1, 1)
    )
    assert len(one_status) == 3
    assert (one_status["system_id"] == 1234).all()


def test_process_system_status_none():

    one_status = process_system_status(
        pv_system_status_text="no status found", date=date(2022, 1, 1)
    )
    assert len(one_status) == 0


def test_process_system_status_less_columns_two_data_points():
    # this has all missing data values
    pv_system_status_text = "1234;07:45,21,255;" "07:45,22,256"
    one_status = process_system_status(
        pv_system_status_text=pv_system_status_text, date=date(2022, 1, 1)
    )
    assert len(one_status) == 2


def test_process_system_status_less_columns_one_data_points():
    # this has all missing data values
    pv_system_status_text = "1234;07:45,21,255"
    one_status = process_system_status(
        pv_system_status_text=pv_system_status_text, date=date(2022, 1, 1)
    )
    assert len(one_status) == 1
    assert np.isnan(one_status["temperature_C"][0])


def test_process_batch_status():
    # Response text copied from
    # https://pvoutput.org/help.html#dataservice-getbatchstatus
    response_text = """
20140330;07:35,2,24;07:40,4,24;07:45,6,24;07:50,8,24;07:55,13,60;08:00,24,132
20140329;07:35,2,24;07:40,4,24;07:45,6,24;07:50,8,24;07:55,13,60;08:00,24,132
20140328;07:35,2,24;07:40,4,24;07:45,6,24;07:50,8,24;07:55,13,60;08:00,24,132"""

    correct_interpretation_csv = """
datetime,cumulative_energy_gen_Wh,instantaneous_power_gen_W,temperature_C,voltage
2014-03-28 07:35:00,2.0,24.0,,
2014-03-28 07:40:00,4.0,24.0,,
2014-03-28 07:45:00,6.0,24.0,,
2014-03-28 07:50:00,8.0,24.0,,
2014-03-28 07:55:00,13.0,60.0,,
2014-03-28 08:00:00,24.0,132.0,,
2014-03-29 07:35:00,2.0,24.0,,
2014-03-29 07:40:00,4.0,24.0,,
2014-03-29 07:45:00,6.0,24.0,,
2014-03-29 07:50:00,8.0,24.0,,
2014-03-29 07:55:00,13.0,60.0,,
2014-03-29 08:00:00,24.0,132.0,,
2014-03-30 07:35:00,2.0,24.0,,
2014-03-30 07:40:00,4.0,24.0,,
2014-03-30 07:45:00,6.0,24.0,,
2014-03-30 07:50:00,8.0,24.0,,
2014-03-30 07:55:00,13.0,60.0,,
2014-03-30 08:00:00,24.0,132.0,,"""

    df = process_batch_status(response_text)
    correct_df = pd.read_csv(
        StringIO(correct_interpretation_csv), parse_dates=["datetime"], index_col="datetime"
    )
    pd.testing.assert_frame_equal(df, correct_df)

    empty_df = process_batch_status("")
    assert empty_df.empty, "DataFrame should be empty but it was:\n{}\n".format(empty_df)

    with pytest.raises(NotImplementedError):
        process_batch_status("20140330;07:35,2,24,2,24,23.1,230.3")
