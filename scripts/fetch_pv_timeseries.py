"""Tool for importing timeseries PV data from PVOutput.

Takes in a metadata and system file, and fetches the
PV data as a hdf5 file as described by the contents of
input.

Typical usage example:

  python fetch_pv_timeseries.py --systemfile system.csv --metafile metafile.csv --outdir /out
"""

from pvoutput import *

import click as cl
import datetime as dt
import sys
import pandas as pd

from typing import NamedTuple


@cl.command()
@cl.option(
    "-m", "--metafile", "metafile_path", envvar="METAFILE", required=True, type=cl.Path(exists=True)
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
@cl.option(
    "-s",
    "--systemfile",
    "systemfile_path",
    envvar="SYSTEMFILE",
    required=True,
    type=cl.Path(exists=True),
)
def run(
    metafile_path: str,
    output_directory: str,
    systemfile_path: str,
    start_date: dt.datetime,
    end_date: dt.datetime,
):
    if end_date < start_date:
        sys.exit("End date cannot occur before start date")
    fetch_metadata(metafile_path)


def fetch_metadata(meta):
    pv_metadata = pd.read_csv(meta, index_col="system_id")
    print(pv_metadata.head())


if __name__ == "__main__":
    run()
