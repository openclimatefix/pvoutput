from datetime import date
from io import StringIO

import numpy as np
import pandas as pd


def process_system_status(pv_system_status_text, date):
    """

    Args:
        pv_system_status_text:
        date:

    Returns:
    """


    # get system id
    system_id = int(pv_system_status_text.split(";")[0])
    pv_system_status_text = ";".join(pv_system_status_text.split(";")[1:])

    # See https://pvoutput.org/help/data_services.html#data-services-get-system-status
    columns = [
        "cumulative_energy_gen_Wh",
        "instantaneous_power_gen_W",
        "temperature_C",
        "voltage",
    ]

    one_pv_system_status = pd.read_csv(
        StringIO(pv_system_status_text),
        lineterminator=";",
        names=["time"] + columns,
        dtype={col: np.float64 for col in columns},
    ).sort_index()

    # process dataframe
    one_pv_system_status["system_id"] = system_id

    # format date
    one_pv_system_status["date"] = date
    one_pv_system_status["date"] = pd.to_datetime(date)

    # format time
    one_pv_system_status["time"] = pd.to_datetime(one_pv_system_status["time"]).dt.strftime(
        "%H:%M:%S"
    )
    one_pv_system_status["time"] = pd.to_timedelta(one_pv_system_status["time"])

    # make datetime
    one_pv_system_status["datetime"] = (
            one_pv_system_status["date"] + one_pv_system_status["time"]
    )
    one_pv_system_status.drop(columns=["date", "time"], inplace=True)

    return one_pv_system_status


def process_batch_status(pv_system_status_text):
    # See https://pvoutput.org/help.html#dataservice-getbatchstatus

    # PVOutput uses a non-standard format for the data.  The text
    # needs some processing before it can be read as a CSV.
    processed_lines = []
    for line in pv_system_status_text.split("\n"):
        line_sections = line.split(";")
        date = line_sections[0]
        time_and_data = line_sections[1:]
        processed_line = [
            "{date},{payload}".format(date=date, payload=payload) for payload in time_and_data
        ]
        processed_lines.extend(processed_line)

    if processed_lines:
        first_line = processed_lines[0]
        num_cols = len(first_line.split(","))
        if num_cols >= 8:
            raise NotImplementedError("Handling of consumption data is not implemented!")

    processed_text = "\n".join(processed_lines)
    del processed_lines

    columns = ["cumulative_energy_gen_Wh", "instantaneous_power_gen_W", "temperature_C", "voltage"]

    pv_system_status = pd.read_csv(
        StringIO(processed_text),
        names=["date", "time"] + columns,
        parse_dates={"datetime": ["date", "time"]},
        index_col=["datetime"],
        dtype={col: np.float64 for col in columns},
    ).sort_index()

    return pv_system_status