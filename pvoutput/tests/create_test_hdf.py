#! /usr/bin/python
import pandas as pd

FILENAME = "test.hdf"
PV_SYSTEM_ID = 123


def get_timeseries_df():
    df = pd.DataFrame(
        index=pd.date_range("2019-01-01", periods=20, freq="5T"),
        columns=["datetime_of_API_request", "query_date", "instantaneous_power_gen_W"],
    )
    df.index.name = "datetime"
    df["datetime_of_API_request"] = [pd.Timestamp("2019-02-01", tz="UTC")] * len(df)
    df["query_date"] = [pd.Timestamp("2019-01-01")] * len(df)
    df["instantaneous_power_gen_W"] = list(range(20))
    return df


def get_missing_dates():
    df = pd.DataFrame(
        [
            [
                PV_SYSTEM_ID,
                pd.Timestamp("2019-01-02"),
                pd.Timestamp("2019-01-02"),
                pd.Timestamp("2019-02-01", tz="UTC"),
            ],
            [
                PV_SYSTEM_ID,
                pd.Timestamp("2019-01-03"),
                pd.Timestamp("2019-01-03"),
                pd.Timestamp("2019-02-01", tz="UTC"),
            ],
        ],
        columns=[
            "pv_system_id",
            "missing_start_date_PV_localtime",
            "missing_end_date_PV_localtime",
            "datetime_of_API_request",
        ],
    ).set_index("pv_system_id")
    return df


def main():
    timeseries = get_timeseries_df()
    missing_dates = get_missing_dates()
    with pd.HDFStore(FILENAME, mode="w") as store:
        store.append(key="/timeseries/{}".format(PV_SYSTEM_ID), value=timeseries, data_columns=True)
        store.append(key="missing_dates", value=missing_dates, data_columns=True)


if __name__ == "__main__":
    main()
