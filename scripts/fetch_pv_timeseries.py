"""Tool for importing timeseries PV data from PVOutput.

Takes in a PVOutput system csv file, and fetches the
PV system Timeseries data as a hdf file as described by the contents of
input, built according to the PVOutput library hdf file spec.
The output file is named according to the inputfile, with
"systems" replaced with "timeseries", e.g.
PVOutput_Albania_systems.csv -> PVOutput_Albania_timeseries.hdf

Typical usage example:

  python fetch_pv_timeseries.py -s system.csv -o out --startdate 2019-07-25 --enddate 2020-07-25

Requirements:

Either: set the env vars
  - DATA_SERVICE_URL
  - PVOUTPUT_AUTH_SYSTEMID
  - PVOUTPUT_AUTH_APIKEY,
pass their equivalent arguments to the command,
or create and use a ~/.pvoutput.yml file as described in the PVOutput library documentation
"""

import datetime as dt
import logging
import pathlib
import sys

import click as cl
import pandas as pd

from pvoutput import *


@cl.command()
@cl.option(
    "-s",
    "--systemfile",
    "systemfile_path",
    envvar="SYSTEMFILE",
    required=True,
    type=cl.Path(exists=True),
)
@cl.option(
    "-o",
    "--outdir",
    "output_directory",
    envvar="OUTDIR",
    default="/mnt/storage_b/data/ocf/solar_pv_nowcasting/nowcasting_dataset_pipeline/PV/PVOutput.org",
    type=cl.Path(exists=False, dir_okay=True),
)
@cl.option(
    "--startdate", "start_date", envvar="STARTDATE", default="2019-05-20", type=cl.DateTime()
)
@cl.option("--enddate", "end_date", envvar="ENDDATE", default="2019-08-20", type=cl.DateTime())
@cl.option("--data_service_url", envvar="DATA_SERVICE_URL")
@cl.option("--pvo_systemid", envvar="PVOUTPUT_AUTH_SYSTEMID", required=True, type=str)
@cl.option("--pvo_apikey", envvar="PVOUTPUT_AUTH_APIKEY", required=True, type=str)
def run(
    output_directory: str,
    systemfile_path: str,
    pvo_systemid: str,
    pvo_apikey: str,
    data_service_url: str,
    start_date: dt.datetime,
    end_date: dt.datetime,
):
    if end_date < start_date:
        sys.exit("End date cannot occur before start date")

    # Create output directory if it doesn't already exist
    os.makedirs(output_directory, exist_ok=True)

    # Instantiate PVOutput library
    pv: pvoutput.PVOutput = PVOutput(
        system_id=pvo_systemid, api_key=pvo_apikey, data_service_url=data_service_url
    )

    # Read in input systemsfile
    pv_systems: pd.DataFrame = pd.read_csv(systemfile_path, index_col="system_id")

    # Write output file
    filename: str = pathlib.Path(systemfile_path).stem.replace("systems", "timeseries") + ".hdf"
    logging.info(f"Writing to {output_directory}/{filename}")
    pv.download_multiple_systems_to_disk(
        system_ids=pv_systems.index,
        start_date=start_date,
        end_date=end_date,
        output_filename=output_directory + "/" + filename,
    )


if __name__ == "__main__":
    run()
