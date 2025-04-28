import logging
import os
from datetime import datetime, timedelta
from typing import List, Optional

import pandas as pd
from nowcasting_datamodel.models.pv import PVSystem, PVSystemSQL
from sqlalchemy.orm import Session

import pvoutput
from pvoutput import PVOutput
from pvoutput.live.utils import df_to_list_pv_system, list_pv_system_to_df

logger = logging.getLogger(__name__)


def load_pv_systems(filename: Optional[str] = None) -> List[PVSystem]:

    if filename is None:
        filename = os.path.dirname(pvoutput.__file__) + "/data/pv_systems.csv"

    pv_capacity = pd.read_csv(filename)

    pv_systems = df_to_list_pv_system(pv_systems_df=pv_capacity)

    return pv_systems


def find_missing_pv_systems(
    pv_systems_local: List[PVSystem], pv_systems_db: List[PVSystem]
) -> List[PVSystem]:
    """
    Find missing pv systems

    Gte the pv systems that are in local file, but not in the database
    Args:
        pv_systems_local: list of pv systems stored locally
        pv_systems_db: list of pv systems from the database

    Returns: list of pv systems that are not in the database

    """

    if len(pv_systems_db) == 0:
        return pv_systems_local

    # change to dataframes
    pv_systems_db = list_pv_system_to_df(pv_systems=pv_systems_db)[["pv_system_id"]]
    pv_systems_local = list_pv_system_to_df(pv_systems=pv_systems_local)[["pv_system_id"]]

    # https://stackoverflow.com/questions/28901683/pandas-get-rows-which-are-not-in-other-dataframe
    # merge together
    df_all = pv_systems_local.merge(
        pv_systems_db.drop_duplicates(), on=["pv_system_id"], how="left", indicator=True
    )

    missing = df_all["_merge"] == "left_only"
    pv_systems_missing = df_all[missing]
    pv_systems_missing["provider"] = "pvoutput.org"

    # add log debug

    return df_to_list_pv_system(pv_systems_missing)


def get_pv_systems(session: Session, filename: Optional[str] = None) -> List[PVSystemSQL]:

    # load all pv systems in database
    pv_systems_sql_db: List[PVSystemSQL] = session.query(PVSystemSQL).all()
    pv_systems_db = [PVSystem.from_orm(pv_system) for pv_system in pv_systems_sql_db]

    # load master file
    pv_system_local = load_pv_systems(filename=filename)

    # get missing pv systems
    missing_pv_system = find_missing_pv_systems(
        pv_systems_local=pv_system_local, pv_systems_db=pv_systems_db
    )

    if len(missing_pv_system) > 0:
        # set up pv output.prg
        pv_output = PVOutput()
        for pv_system in missing_pv_system:

            # get metadata
            metadata = pv_output.get_metadata(
                pv_system_id=pv_system.pv_system_id, use_data_service=True
            )
            logger.info(
                f"Fpr py stsem {pv_system.pv_system_id}, setting "
                f"latitude {metadata.latitude}, "
                f"longitude {metadata.longitude}, "
                f"status_interval_minutes {metadata.status_interval_minutes}, "
            )
            pv_system.latitude = metadata.latitude
            pv_system.longitude = metadata.longitude
            pv_system.status_interval_minutes = int(metadata.status_interval_minutes)

            # validate
            _ = PVSystem.from_orm(pv_system)

            # add to database
            session.add(pv_system.to_orm())

    return session.query(PVSystemSQL).all()


def filter_pv_systems_which_have_new_data(
    pv_systems: List[PVSystemSQL], datetime_utc: Optional[datetime] = None
):
    """
    Filter pv systems which have new data available

    This is done by looking at the datestamp of last data pulled,
    add then by looking at the pv system refresh time, we can determine if new data is available

    sudo code:
        if last_datestamp + refresh_interval > datetime_now
            keep = True

    Args:
        pv_systems: list of pv systems
        datetime_utc: the datetime now

    Returns: list of pv systems that have new data.

    """

    logger.info(
        f"Looking at which PV systems might have new data. "
        f"Number of pv systems are {len(pv_systems)}"
    )

    if datetime_utc is None:
        datetime_utc = datetime.utcnow()  # add timezone

    keep_pv_systems = []
    for pv_system in pv_systems:

        if pv_system.status_interval_minutes is None:
            # don't know the status interval refresh time, so lets keep it
            logger.debug(
                f"We dont know the refresh time for pv systems {pv_system.pv_system_id}, "
                f"so will be getting data "
            )
            keep_pv_systems.append(pv_system)
        elif pv_system.last_pv_yield is None:
            # there is no pv yield data for this pv system, so lets keep it
            logger.debug(
                f"There is no pv yield data for pv systems {pv_system.pv_system_id}, "
                f"so will be getting data "
            )
            keep_pv_systems.append(pv_system)
        else:
            next_datetime_data_available = (
                timedelta(minutes=pv_system.status_interval_minutes)
                + pv_system.last_pv_yield.datetime_utc
            )
            if next_datetime_data_available < datetime_utc:
                logger.debug(
                    f"For pv system {pv_system.pv_system_id} as "
                    f"last pv yield datetime is {pv_system.last_pv_yield.datetime_utc},"
                    f"refresh interval is {pv_system.status_interval_minutes}, "
                    f"so will be getting data"
                )
                keep_pv_systems.append(pv_system)
            else:
                logger.debug(
                    f"Not keeping pv system {pv_system.pv_system_id} as "
                    f"last pv yield datetime is {pv_system.last_pv_yield.datetime_utc},"
                    f"refresh interval is {pv_system.status_interval_minutes}"
                )

    return keep_pv_systems
