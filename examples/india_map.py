""" Example of plotting PVOutput India system locations on a map."""
import os
import plotly.express as px
import pandas as pd

# load csv file with system metadata
pv_system_metadata = "./examples/pv_data/PVOutput_India_systems.csv"
# pv_system_metadata = os.environ.get("PV_SYSTEM_FILE")
pv_system_metadata = pd.read_csv(pv_system_metadata)
pv_systems_lat_lon = pd.DataFrame(
    pv_system_metadata, columns=["system_id", "latitude", "longitude", "system_size_W"]
)
# remove systems that don't have a lat/lon coordinate
if pv_systems_lat_lon["latitude"].isnull().values.any():
    pv_systems_lat_lon = pv_systems_lat_lon.dropna()


fig = px.scatter_geo(
    pv_systems_lat_lon,
    lat="latitude",
    lon="longitude",
    size="system_size_W",
    color="system_size_W",
    hover_name="system_id",
    scope="asia",
    title="PVOutput India System Locations",
)
fig.show()
