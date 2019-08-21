import pandas as pd
from pvoutput import daterange
from pvoutput.daterange import DateRange
from datetime import date


def test_get_date_range_list():
    def _get_date_range(start_date, periods):
        return list(pd.date_range(start_date, periods=periods, freq="D"))
    dates = []
    for start_date, periods in [
            ("2019-01-01", 5), ("2019-05-01", 3), ("2015-04-01", 1)]:
        dates.extend(_get_date_range(start_date, periods))

    date_range_list = daterange.get_date_range_list(dates)
    assert date_range_list[0].start_date == date(2015, 4, 1)
    assert date_range_list[0].end_date == date(2015, 4, 1)

    assert date_range_list[1].start_date == date(2019, 1, 1)
    assert date_range_list[1].end_date == date(2019, 1, 5)

    assert date_range_list[2].start_date == date(2019, 5, 1)
    assert date_range_list[2].end_date == date(2019, 5, 3)

    assert daterange.get_date_range_list([]) == []


def test_intersection():
    assert DateRange("2019-01-01", "2019-01-02").intersection(
        DateRange("2020-01-01", "2020-01-02")) is None

    assert DateRange("2019-01-01", "2019-01-10").intersection(
        DateRange("2019-01-01", "2019-01-02")) == DateRange(
            "2019-01-01", "2019-01-02")

    assert DateRange("2019-01-01", "2019-01-10").intersection(
        DateRange("2019-01-05", "2019-01-20")) == DateRange(
            "2019-01-05", "2019-01-10")
