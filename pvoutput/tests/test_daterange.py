from datetime import date

import pandas as pd

from pvoutput import daterange
from pvoutput.daterange import DateRange, merge_date_ranges_to_years


def test_get_date_range_list():
    def _get_date_range(start_date, periods):
        return list(pd.date_range(start_date, periods=periods, freq="D"))

    dates = []
    for start_date, periods in [("2019-01-01", 5), ("2019-05-01", 3), ("2015-04-01", 1)]:
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
    assert (
        DateRange("2019-01-01", "2019-01-02").intersection(DateRange("2020-01-01", "2020-01-02"))
        is None
    )

    assert DateRange("2019-01-01", "2019-01-10").intersection(
        DateRange("2019-01-01", "2019-01-02")
    ) == DateRange("2019-01-01", "2019-01-02")

    assert DateRange("2019-01-01", "2019-01-10").intersection(
        DateRange("2019-01-05", "2019-01-20")
    ) == DateRange("2019-01-05", "2019-01-10")

    year = DateRange("2018-01-1", "2019-01-01")
    dec = DateRange("2018-12-01", "2019-01-01")
    assert year.intersection(dec) == dec

    june = DateRange("2018-06-01", "2018-07-01")
    assert year.intersection(june) == june

    incomplete_overlap = DateRange("2017-07-01", "2018-02-01")
    assert year.intersection(incomplete_overlap) != incomplete_overlap


def test_total_days():
    assert DateRange("2019-01-01", "2019-01-10").total_days() == 9


def test_split_into_years():
    short_dr = DateRange("2019-01-01", "2019-01-10")
    assert short_dr.split_into_years() == [short_dr]

    one_year = DateRange("2019-01-01", "2020-01-01")
    assert one_year.split_into_years() == [one_year]

    year_and_half = DateRange("2019-01-01", "2020-06-01")
    assert year_and_half.split_into_years() == [
        DateRange("2019-06-02", "2020-06-01"),
        DateRange("2019-01-01", "2019-06-02"),
    ]


def test_merge_date_ranges_to_years():
    jan = DateRange("2018-01-01", "2018-02-01")
    multiyear = DateRange("2017-01-01", "2018-02-01")
    old_multiyear = DateRange("2014-01-01", "2016-02-01")
    ancient_jan = DateRange("2010-01-01", "2010-02-01")
    for date_ranges, merged in [
        ([], []),
        ([jan], [DateRange("2017-02-01", "2018-02-01")]),
        (
            [multiyear],
            [DateRange("2017-02-01", "2018-02-01"), DateRange("2016-02-02", "2017-02-01")],
        ),
        (
            [old_multiyear, multiyear],
            [
                DateRange("2017-02-01", "2018-02-01"),
                DateRange("2016-02-02", "2017-02-01"),
                DateRange("2015-02-01", "2016-02-01"),
                DateRange("2014-02-01", "2015-02-01"),
                DateRange("2013-02-01", "2014-02-01"),
            ],
        ),
        (
            [ancient_jan, old_multiyear, multiyear],
            [
                DateRange("2017-02-01", "2018-02-01"),
                DateRange("2016-02-02", "2017-02-01"),
                DateRange("2015-02-01", "2016-02-01"),
                DateRange("2014-02-01", "2015-02-01"),
                DateRange("2013-02-01", "2014-02-01"),
                DateRange("2009-02-01", "2010-02-01"),
            ],
        ),
    ]:
        assert merge_date_ranges_to_years(date_ranges) == merged
