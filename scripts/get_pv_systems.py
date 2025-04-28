""" Get list of pv systems data for live

1. load data from gcp
2. save systems ids to a txt file

You may need to
pip install h5netcdf


"""

filename_metadata = "gs://solar-pv-nowcasting-data/PV/PVOutput.org/UK_PV_metadata.csv"
filename = "gs://solar-pv-nowcasting-data/PV/PVOutput.org/UK_PV_timeseries_batch.nc"

import io

import fsspec
import pandas as pd
import xarray as xr

# dont need this, but nice to look sometimes
metedata = pd.read_csv(filename_metadata)

with fsspec.open(filename, mode="rb") as file:
    file_bytes = file.read()

with io.BytesIO(file_bytes) as file:
    pv_power = xr.open_dataset(file, engine="h5netcdf")
    max_power = pv_power.max()
    systems_ids = list(pv_power.keys())

# convert to simple dataframe with system id as the index, and capacity as a column
max_power_df = pd.DataFrame(max_power.to_dict()["data_vars"]).T
max_power_df = max_power_df[["data"]].rename(columns={"data": "capacity_kw"})
max_power_df["provider"] = "pvoutput.org"
max_power_df["pv_system_id"] = max_power_df.index

# save
# TODO save on s3
max_power_df.to_csv("pvoutput/data/pv_systems.csv")
