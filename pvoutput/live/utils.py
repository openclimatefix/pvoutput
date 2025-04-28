from typing import List

import pandas as pd
from nowcasting_datamodel.models import PVSystem


def list_pv_system_to_df(pv_systems: List[PVSystem]) -> pd.DataFrame:
    """
    Change list of pv systems to dataframe

    Args:
        pv_systems: list of pv systems (pdyantic objects)

    Returns: dataframe with columns the same as the pv systems pydantic object

    """
    return pd.DataFrame([pv_system.dict() for pv_system in pv_systems])


def df_to_list_pv_system(pv_systems_df=pd.DataFrame) -> List[PVSystem]:
    """
    Change dataframe to lsit of pv systems

    Args:
        pv_systems_df: dataframe with columns the same as the pv systems pydantic object

    Returns: list of pv systems

    """
    return [PVSystem(**row) for row in pv_systems_df.to_dict(orient="records")]
