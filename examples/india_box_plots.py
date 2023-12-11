"""Box plots of max cumulative energy generation per system"""
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import h5py

# load hdf file with the generation data for each system
pv_systems_hdf = os.environ.get("PV_DATA_HDF")

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


pv_systems = []

with h5py.File(pv_systems_hdf, "r") as f:
    for system_id in systems_with_data:
        df_pv_system = pd.DataFrame(np.array(f["timeseries"][system_id]["table"]))
        df_pv_system["index"] = pd.to_datetime(df_pv_system["index"], unit="ns")
        df_pv_system = df_pv_system.groupby(pd.Grouper(key="index", freq="D")).max()
        df_pv_system["System ID"] = system_id
        df_pv_system = pd.DataFrame(
            df_pv_system, columns=["cumulative_energy_gen_Wh", "System ID"]
        )
        df_pv_system["cumulative_energy_gen_kWh"] = (
            df_pv_system["cumulative_energy_gen_Wh"] / 1000
        ).astype(float)
        pv_systems.append(df_pv_system)


fig = go.Figure()
for pv_system in pv_systems:
    fig.add_trace(
        go.Box(
            y=pv_system["cumulative_energy_gen_kWh"],
            x=pv_system["System ID"],
            name=pv_system["System ID"][0],
            boxpoints="suspectedoutliers",
            jitter=0.5,
            whiskerwidth=0.2,
            fillcolor="rgba(93, 164, 214, 0.5)",
            marker_size=2,
            line_width=1,
        )
    )
    fig.update_layout(
        title="Daily max values of cumulative energy generation per system",
        yaxis=dict(
            autorange=True,
            showgrid=True,
            zeroline=True,
            gridcolor="rgb(255, 255, 255)",
            gridwidth=1,
            zerolinecolor="rgb(255, 255, 255)",
            zerolinewidth=2,
        ),
    ),

    y_data = df_pv_system["cumulative_energy_gen_kWh"]
    x_data = df_pv_system["System ID"]

    colors = [
        "rgba(93, 164, 214, 0.5)",
        "rgba(255, 144, 14, 0.5)",
        "rgba(44, 160, 101, 0.5)",
        "rgba(255, 65, 54, 0.5)",
        "rgba(207, 114, 255, 0.5)",
        "rgba(127, 96, 0, 0.5)",
    ]

    margin = (
        dict(
            l=40,
            r=30,
            b=80,
            t=100,
        ),
    )
    paper_bgcolor = ("rgb(243, 243, 243)",)
    plot_bgcolor = ("rgb(243, 243, 243)",)
    showlegend = (False,)
    title = ("Daily Max Production per System for India",)
    xaxis = (
        dict(
            autorange=True,
            showgrid=True,
            zeroline=True,
            dtick=5,
            gridcolor="rgb(255, 255, 255)",
            gridwidth=1,
            zerolinecolor="rgb(255, 255, 255)",
            zerolinewidth=2,
        ),
    )

fig.show()
