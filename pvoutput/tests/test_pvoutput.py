import pandas as pd
from pvoutput import pvoutput


def test_convert_consecutive_dates_to_date_ranges():
    dr1 = pd.date_range("2018-01-01", "2018-02-01", freq="D").tolist()
    dr2 = pd.date_range("2018-02-05", "2018-02-10", freq="D").tolist()
    missing_dates = dr1 + dr2
    date_ranges = pvoutput._convert_consecutive_dates_to_date_ranges(
        missing_dates)

    pd.testing.assert_frame_equal(
        date_ranges,
        pd.DataFrame(
            [
                [dr1[0], dr1[-1]],
                [dr2[0], dr2[-1]],
            ],
            columns=[
                'missing_start_date_PV_localtime',
                'missing_end_date_PV_localtime'
            ]))
