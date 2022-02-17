from pvoutput.live.app import pull_data, app

from nowcasting_datamodel.models.pv import PVSystem, PVSystemSQL, PVYieldSQL, PVYield
from datetime import datetime, timezone
from typing import List

import os
import pvoutput


def test_pull_data(db_session):

    pv_systems = [
        PVSystem(pv_system_id=10003, provider="pvoutput.org").to_orm(),
    ]

    pv_yields = pull_data(pv_systems=pv_systems, session=db_session)

    assert len(pv_yields) > 0

# def test_app():
#
#
#