""" Function to process data """
import logging
from io import StringIO

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def process_system_status(pv_system_status_text, date) -> pd.DataFrame:
    """
    Process raw system status

    Args:
        pv_system_status_text: string of system data, like:
            "1234;07:45,21,255,1,2;07:50,21,255,1;07:50,21,255,1,2"
        date: The date this data is from

    Returns: dataframe of data
    """

    # See https://pvoutput.org/help/data_services.html#data-services-get-system-status
    columns = [
        "cumulative_energy_gen_Wh",
        "instantaneous_power_gen_W",
        "temperature_C",
        "voltage",
    ]
    if pv_system_status_text == "no status found":
        logger.debug("Text was empty so return empty dataframe")
        return pd.DataFrame(columns=columns + ["system_id", "datetime"])

    # get system id
    system_id = int(pv_system_status_text.split(";")[0])
    pv_system_status_text = ";".join(pv_system_status_text.split(";")[1:])

    try:
        one_pv_system_status = pd.read_csv(
            StringIO(pv_system_status_text),
            lineterminator=";",
            names=["time"] + columns,
            dtype={col: np.float64 for col in columns},
        ).sort_index()

    except Exception as e:

        # this can happen if there is only one data value and it doesnt contain all 5 columns.
        # if there is many rows of data, then it seems fine
        if pv_system_status_text.count(";") != 0:
            # the data contains more than one row, so lets raise the error
            raise e

        # how many columns does it have
        n_columns = pv_system_status_text.count(",") + 1

        one_pv_system_status = pd.read_csv(
            StringIO(pv_system_status_text),
            lineterminator=";",
            names=["time"] + columns[: n_columns - 1],
            dtype={col: np.float64 for col in columns},
        ).sort_index()

        missing_columns = [c for c in columns if c not in one_pv_system_status.columns]
        one_pv_system_status[missing_columns] = np.NAN

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
    one_pv_system_status["datetime"] = one_pv_system_status["date"] + one_pv_system_status["time"]
    one_pv_system_status.drop(columns=["date", "time"], inplace=True)

    return one_pv_system_status


def process_batch_status(pv_system_status_text) -> pd.DataFrame:
    """
    Process batch status text

    Args:
        pv_system_status_text: text to be procssed

    Returns: dataframe of data

    """
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
