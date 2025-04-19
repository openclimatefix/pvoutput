"""Histogram of system capacities for India."""
import os

import pandas as pd
import plotly.express as px

# load csv file with system metadata
pv_systems_metadata = os.environ.get("PV_SYSTEM_METADATA")

# read the csv file and build a dataframe
data = pd.read_csv(pv_systems_metadata)
pv_metadata = pd.DataFrame(data, columns=["system_id", "system_size_W"])
pv_metadata["system_id"] = pv_metadata["system_id"].astype(str)
pv_metadata["system_size_W"] = (pv_metadata["system_size_W"] / 1000).astype(float)
pv_metadata.rename(
    columns={"system_id": "System ID", "system_size_W": "System Capacity (kW)"},
    inplace=True,
)

# plot the data as histogram in plotly
fig = px.histogram(
    pv_metadata,
    x="System ID",
    y="System Capacity (kW)",
    title="PVOutput India System Capacities",
)
fig.show()
