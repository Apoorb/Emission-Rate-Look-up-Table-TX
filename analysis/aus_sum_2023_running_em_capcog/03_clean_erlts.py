import pandas as pd
import os
from ttierlt_v1.utils import (
    PATH_PROCESSED,
)


path_aus_sum_2023_running_out = os.path.join(
    PATH_PROCESSED,
    "aus_sum_2023_running_erlt",
    "aus_sum_2023_running_erlt_extra_pols.xlsx",
)

path_aus_sum_2023_running_out_2 = os.path.join(
    PATH_PROCESSED,
    "aus_sum_2023_running_erlt",
    "aus_sum_2023_running_erlt_extra_pols_red_rdtypes.xlsx",
)

aus_sum_2023_running_out = pd.read_excel(path_aus_sum_2023_running_out, index_col=0)

funclass_nm_map = {
    "Rural-Freeway": "Restricted Access",
    "Urban-Freeway": "Restricted Access",
    "Rural-Arterial": "Unrestricted Access",
    "Urban-Arterial": "Unrestricted Access",
}

funclass_rur_urb_prop_map = {
    "Rural-Freeway": 0.010238999,
    "Urban-Freeway": 0.989761001,
    "Rural-Arterial": 0.143757254,
    "Urban-Arterial": 0.892610835,
}


aus_sum_2023_running_out_1 = (
    aus_sum_2023_running_out.set_index(
        ["Area", "yearid", "monthid", "hourid", "funclass", "avgspeed"]
    )
    .stack()
    .reset_index()
    .rename(columns={"level_6": "pollutants", 0: "emissions"})
)


aus_sum_2023_running_out_2 = (
    aus_sum_2023_running_out_1.assign(
        funclass_prop=lambda df: df.funclass.map(funclass_rur_urb_prop_map),
        funclass_red=lambda df: df.funclass.map(funclass_nm_map),
        weight_emissions=lambda df: df.emissions * df.funclass_prop,
        hourid=lambda df: "Hour " + df.hourid.astype(str),
    )
    .groupby(
        [
            "Area",
            "yearid",
            "monthid",
            "hourid",
            "funclass_red",
            "avgspeed",
            "pollutants",
        ]
    )
    .weight_emissions.sum()
    .unstack()
    .reset_index()
    .filter(
        items=[
            "Area",
            "yearid",
            "monthid",
            "hourid",
            "funclass_red",
            "avgspeed",
            "CO",
            "NOX",
            "NH3",
            "SO2",
            "PM10",
            "PM10_Brakewear",
            "PM10_Tirewear",
            "PM25",
            "PM25_Brakewear",
            "PM25_Tirewear",
            "VOC",
            "CO2",
            "Organic_Carbon",
        ]
    )
    .rename(
        columns={
            "NOX": "NOx",
            "PM25": "PM2.5",
            "PM10_Brakewear": "PM10 Brakewear",
            "PM10_Tirewear": "PM10 Tirewear",
            "PM25_Brakewear": "PM2.5 Brakewear",
            "PM25_Tirewear": "PM2.5 Tirewear",
            "Organic_Carbon": "Organic Carbon",
        }
    )
)


aus_sum_2023_running_out_2.to_excel(path_aus_sum_2023_running_out_2, index=False)
