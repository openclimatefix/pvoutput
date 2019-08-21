import os
import inspect
import numpy as np
import pandas as pd
from pvoutput import utils
from pvoutput.daterange import DateRange
from datetime import date


def data_dir():
    # Taken from http://stackoverflow.com/a/6098238/732596
    data_dir = os.path.dirname(inspect.getfile(inspect.currentframe()))
    data_dir = os.path.abspath(data_dir)
    assert os.path.isdir(data_dir), data_dir + " does not exist."
    return data_dir


TEST_HDF = os.path.join(data_dir(), 'test.hdf')
PV_SYSTEM = 123


def test_get_missing_dates_for_id():
    missing_dates = utils.get_missing_dates_for_id(TEST_HDF, PV_SYSTEM)
    np.testing.assert_array_equal(
        missing_dates,
        [date(2019, 1, 2), date(2019, 1, 3)])


def test_get_system_ids_in_store():
    system_ids = utils.get_system_ids_in_store(TEST_HDF)
    np.testing.assert_array_equal(system_ids, [PV_SYSTEM])


def test_get_date_ranges_to_download():
    date_ranges = utils.get_date_ranges_to_download(
        TEST_HDF, PV_SYSTEM, '2018-01-01', '2019-01-10')
    np.testing.assert_array_equal(
        date_ranges,
        [DateRange(
            start_date=date(2018, 1, 1),
            end_date=date(2018, 12, 31)),
         DateRange(
             start_date=date(2019, 1, 4),
             end_date=date(2019, 1, 10))]
        )


def test_datetime_list_to_dates():
    np.testing.assert_array_equal(
        utils.datetime_list_to_dates(pd.Timestamp("2019-01-01")),
        [date(2019, 1, 1)])

    np.testing.assert_array_equal(
        utils.datetime_list_to_dates([
            pd.Timestamp("2019-01-01"), pd.Timestamp("2019-01-02")]),
        [date(2019, 1, 1), date(2019, 1, 2)])
