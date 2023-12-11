"""Gantt chart for India PV systems. This shows where there are gaps in the data."""
import os
import pandas as pd
import numpy as np
import plotly.express as px
import h5py

# load hdf file with the generation data for each system
pv_data_hdf = os.environ.get("PV_DATA_HDF")

# these are the current systems with some data in the hdf file for india
systems_with_data = [
    "56151",
    "56709",
    "58780",
    "59687",
    "59710",
    "60294",
    "60602",
    "66634",
    "71120",
    "72742",
    "73347",
    "77684",
    "77710",
    "78186",
    "79612",
    "81408",
    "82081",
    "85738",
    "86244",
    "87410",
    "90559",
    "91554",
    "97094",
    "99833",
    "100451",
]


pv_systems = []

# read the hdf file and get start and end dates of available data per site
with h5py.File(pv_data_hdf, "r") as f:
    # loop through each pv system in the hdf file
    for system_id in systems_with_data:
        df = pd.DataFrame(np.array(f["timeseries"][system_id]["table"]))
        df["index"] = pd.to_datetime(df["index"], unit="ns")
        df = df[df["index"] > pd.Timestamp("2018-01-01")]
        # set a value for the end date otherwise it registers as NaT
        end_date = df["index"].iloc[-1]
        df["index_difference"] = df["index"].diff()
        # get startpoints of gaps in the data
        df = df[df["index_difference"] > pd.Timedelta("1D")]
        # get endpoints of gaps by looking at the difference between indexes
        df["previous_endpoint"] = df["index"] - df["index_difference"]
        df["endpoints"] = df["previous_endpoint"].shift(-1)
        # set the last endpoint to the end date otherwise it registers as NaT
        if len(df["endpoints"]) > 0:
            df["endpoints"].iloc[-1] = end_date

        # make a dictionary for the gantt chart to plot
        # loop over the start and end dates and add to start_end_data dictionary
        for index, row in df.iterrows():
            start_end_data = {}
            start_end_data["System ID"] = system_id
            start_end_data["Start"] = row["index"]
            start_end_data["Finish"] = row["endpoints"]

            pv_systems.append(start_end_data)

    # plot the data as gantt chart in plotly
    fig = px.timeline(
        pv_systems,
        x_start="Start",
        x_end="Finish",
        y="System ID",
        color="System ID",
        title="Gantt Chart of PV Systems in India",
    )
    fig.show()
