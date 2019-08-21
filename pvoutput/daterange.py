from dataclasses import dataclass
from datetime import date, timedelta, datetime
from typing import Iterable, Union, List
import pandas as pd
import numpy as np


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
        return np.timedelta64(
            self.end_date - self.start_date).astype(
                'timedelta64[D]').astype(np.float32)


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
        date_range = DateRange(
            start_date=dates[start_i],
            end_date=dates[end_i])
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
