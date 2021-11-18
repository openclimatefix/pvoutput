from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Iterable, List, Union

import numpy as np
import pandas as pd


@dataclass
class DateRange:
    start_date: date
    end_date: date

    def __init__(self, start_date, end_date):
        self.start_date = safe_convert_to_date(start_date)
        self.end_date = safe_convert_to_date(end_date)

    def intersection(self, other):
        new_start = max(self.start_date, other.start_date)
        new_end = min(self.end_date, other.end_date)
        if new_start < new_end:
            return DateRange(new_start, new_end)

    def date_range(self) -> np.array:
        return pd.date_range(self.start_date, self.end_date, freq="D").date

    def total_days(self) -> int:
        return (
            np.timedelta64(self.end_date - self.start_date)
            .astype("timedelta64[D]")
            .astype(np.float32)
        )

    def split_into_years(self) -> List:
        duration = self.end_date - self.start_date
        num_years = duration / timedelta(days=365)
        if num_years <= 1:
            return [self]
        else:
            end_date = self.end_date
            new_date_ranges = []
            for year_back in range(np.ceil(num_years).astype(int)):
                start_date = end_date - timedelta(days=365)
                if start_date < self.start_date:
                    start_date = self.start_date
                new_date_ranges.append(DateRange(start_date, end_date))
                end_date = start_date
            return new_date_ranges


def get_date_range_list(dates: Iterable[date]) -> List[DateRange]:
    if not dates:
        return []
    dates = np.array(dates)
    dates = np.sort(dates)
    dates_diff = np.diff(dates)
    location_of_gaps = np.where(dates_diff > timedelta(days=1))[0]
    index_of_last_date = len(dates) - 1
    location_of_gaps = list(location_of_gaps)
    location_of_gaps.append(index_of_last_date)

    start_i = 0
    date_range_list = []
    for end_i in location_of_gaps:
        date_range = DateRange(start_date=dates[start_i], end_date=dates[end_i])
        date_range_list.append(date_range)
        start_i = end_i + 1

    return date_range_list


def safe_convert_to_date(dt: Union[datetime, date, str]) -> date:
    if isinstance(dt, str):
        dt = pd.Timestamp(dt)
    if isinstance(dt, datetime):
        return dt.date()
    if isinstance(dt, date):
        return dt


def merge_date_ranges_to_years(date_ranges: Iterable[DateRange]) -> List[DateRange]:
    """
    Args:
        date_ranges: List of DateRanges, in ascending chronological order.

    Returns:
        List of DateRanges, each representing a year, in descending order.
    """
    if not date_ranges:
        return []

    # Split multi-year date ranges
    date_ranges_split = []
    for date_range in date_ranges[::-1]:
        date_ranges_split.extend(date_range.split_into_years())

    years_to_download = []
    for date_range in date_ranges_split:
        if years_to_download:
            intersection = date_range.intersection(years_to_download[-1])
            if intersection == date_range:
                # date_range falls within the last year to retrieve,
                # so we can ignore this date_range
                continue
            elif intersection is None:
                # No overlap
                date_to = date_range.end_date
            else:
                # Overlap
                date_to = intersection.start_date

        else:
            date_to = date_range.end_date

        date_from = date_to - timedelta(days=365)
        years_to_download.append(DateRange(date_from, date_to))

    return years_to_download
