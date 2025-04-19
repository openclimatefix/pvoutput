"""Mean Production per System by time interval (month, week,etc.)
This example shows the mean daily production per system by month.
This makes a plot with 2 columns and half as many rows as there are systems with data.
"""
import os

import h5py
import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# load hdf file with the generation data for each system
pv_data_hdf_file = os.environ.get("SYSTEM_DATA")

# these are the current systems with data in the hdf file for India
systems_with_data = [
    "56151",
    "56709",
    "58780",
    "59687",
    "59710",
    "60294",
    "60602",
    "60673",
    "66634",
    "67861",
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
]


# for the subplot titles, this function is used to get the row number
def row(row):
    for row in range(0, len(pv_systems)):
        if i == 1:
            row = 1
        elif i % 2 == 0:
            row = int(i / 2)
        else:
            row = int((i + 1) / 2)
    return row


pv_systems = []
# read the hdf file
with h5py.File(pv_data_hdf_file, "r") as f:
    # loop through each pv system in the hdf file. some of the lines are commented out but can
    # be used to filter the data by date or to get the mean weekly production per system
    for system_id in systems_with_data:
        df = pd.DataFrame(np.array(f["timeseries"][system_id]["table"]))
        df["index"] = pd.to_datetime(df["index"], unit="ns")
        # df["index"] = df[df["index"] > pd.Timestamp("2019-01-01")]
        df_pv_system = df.groupby(pd.Grouper(key="index", freq="M")).mean()
        df_pv_system["System ID"] = system_id
        df_pv_system = pd.DataFrame(df_pv_system, columns=["cumulative_energy_gen_Wh", "System ID"])
        # convert Wh to kWh
        df_pv_system["cumulative_energy_gen_kWh"] = (
            df_pv_system["cumulative_energy_gen_Wh"] / 1000
        ).astype(float)
        pv_systems.append(df_pv_system)
    i = 1
    # make the plot with subplots
    fig = make_subplots(
        rows=len(pv_systems),
        cols=2,
        shared_xaxes=False,
        horizontal_spacing=0.2,
        vertical_spacing=0.02,
        subplot_titles=[system_id for system_id in systems_with_data],
    )
    # loop through each system and add a line to the subplot
    for i in range(1, len(pv_systems)):
        if len(pv_systems[i - 1]) > 0:
            fig.add_trace(
                go.Scatter(
                    x=pv_systems[i - 1].index,
                    y=pv_systems[i - 1]["cumulative_energy_gen_Wh"],
                    name=pv_systems[i - 1]["System ID"][0],
                    mode="lines",
                ),
                row=row(i),
                col=[2 if i % 2 == 0 else 1],
            )
        i += 1
    fig.update_yaxes(title_text="kWh")
    fig.update_layout(height=3000, width=750, title_text="Mean Monthly Production per System")
    fig.update_annotations(font_size=12)
    fig.show()
