import os
from datetime import date

import numpy as np
import pandas as pd
import pytest

from pvoutput import utils
from pvoutput.daterange import DateRange

PV_SYSTEM = 123


def test_get_missing_dates_for_id(data_dir):
    test_hdf = os.path.join(data_dir, "data/test.hdf")
    missing_dates = utils.get_missing_dates_for_id(test_hdf, PV_SYSTEM)
    np.testing.assert_array_equal(missing_dates, [date(2019, 1, 2), date(2019, 1, 3)])


def test_get_system_ids_in_store(data_dir):
    test_hdf = os.path.join(data_dir, "data/test.hdf")
    system_ids = utils.get_system_ids_in_store(test_hdf)
    np.testing.assert_array_equal(system_ids, [PV_SYSTEM])


def test_get_date_ranges_to_download(data_dir):
    test_hdf = os.path.join(data_dir, "data/test.hdf")
    date_ranges = utils.get_date_ranges_to_download(test_hdf, PV_SYSTEM, "2018-01-01", "2019-01-10")
    # 2018-01-02 and 2018-01-03 are already known to be missing.
    np.testing.assert_array_equal(
        date_ranges,
        [
            DateRange(start_date=date(2018, 1, 1), end_date=date(2018, 12, 31)),
            DateRange(start_date=date(2019, 1, 4), end_date=date(2019, 1, 10)),
        ],
    )


def test_datetime_list_to_dates():
    np.testing.assert_array_equal(
        utils.datetime_list_to_dates(pd.Timestamp("2019-01-01")), [date(2019, 1, 1)]
    )

    np.testing.assert_array_equal(
        utils.datetime_list_to_dates([pd.Timestamp("2019-01-01"), pd.Timestamp("2019-01-02")]),
        [date(2019, 1, 1), date(2019, 1, 2)],
    )
