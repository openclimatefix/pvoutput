from nowcasting_datamodel.fake import make_fake_pv_system
from nowcasting_datamodel.models.pv import PVSystem

from pvoutput.live.utils import df_to_list_pv_system, list_pv_system_to_df


def test_list_pv_system_to_df():
    pv_systems_1 = PVSystem.from_orm(make_fake_pv_system())
    pv_systems_2 = PVSystem.from_orm(make_fake_pv_system())

    _ = list_pv_system_to_df([pv_systems_1, pv_systems_2])


def test_df_to_list_pv_system():
    pv_systems_1 = PVSystem.from_orm(make_fake_pv_system())
    pv_systems_2 = PVSystem.from_orm(make_fake_pv_system())

    df = list_pv_system_to_df([pv_systems_1, pv_systems_2])
    _ = df_to_list_pv_system(df)
