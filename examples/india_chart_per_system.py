"""Chart generation for a single system. This shows the cumulative energy
generation or instantaneous power generation over time. Some of the commented out code can be used to filter the data by date or plot max, mean, min, median values, etc.
"""
import os

import h5py
import numpy as np
import pandas as pd
import plotly.express as px

# load hdf file with the generation data for each system
pv_systems_hdf = os.environ.get("PV_DATA_HDF")

# this plots one system as a line chart in plotly
# plot the data as line graph in plotly

# can choose a system id to input here
system_id = "SYSTEM_ID"
# read the hdf file and get the data for the system id
with h5py.File(pv_systems_hdf, "r") as f:
    pv_system_data = pd.DataFrame(np.array(f["timeseries"][system_id]["table"]))
    pv_system_data["index"] = pd.to_datetime(pv_system_data["index"], unit="ns")
    pv_system_data = pv_system_data.set_index("index", inplace=False)
    # this code can be used to get the mean weekly ("W") or monthly ("M")
    # production per system or the max daily production ("D")
    # pv_system_data = pv_system_data.groupby(pd.Grouper(key="index", freq="M").mean

    pv_system_data["System ID"] = system_id
    pv_system_data = pd.DataFrame(
        pv_system_data,
        columns=[
            "index",
            "cumulative_energy_gen_Wh",
            "System ID",
            "instantaneous_power_gen_W",
        ],
    )
    # filter the data by date
    # pv_system_data = pv_system_data.loc[
    #     pv_system_data.index > pd.Timestamp("2019-05-01 00:00:00")
    # ]
    # pv_system_data = pv_system_data.loc[
    #     pv_system_data.index < pd.Timestamp("2019-07-01 00:00:00")
    # ]

    pv_system_data["cumulative_energy_gen_kWh"] = (
        pv_system_data["cumulative_energy_gen_Wh"] / 1000
    ).astype(float)

    # plot the data as a line chart
    fig = px.line(
        pv_system_data,
        x=pv_system_data.index,
        # here you can swap between cumulative energy generation and instantaneous
        # power generation on the y axis
        y=pv_system_data["cumulative_energy_gen_kWh"],
        title=f"Generation for System ID: {system_id}",
    )
    fig.update_layout(
        xaxis_title="Time",
    )
    fig.show()
