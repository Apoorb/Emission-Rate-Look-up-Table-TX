"""
Interpolate emission rate for intermediate (odd) years.
Interpolate emission rate for intermediate speeds using the MOBILE 6.2 interpolation
formula. It uses inverse of the speed to interpolate.
Created by: Apoorba Bibeka
Date Created: 05/23/2021
"""
import pandas as pd
import numpy as np
import os
from ttierlt.utils import (
    connect_to_server_db,
    get_engine_to_output_to_db,
    PATH_PROCESSED,
)
from ttierlt.yr_spd_interpol import (
    out_yr_spd_interpolated,
)

POLLUTANT_COLS = [
    "CO",
    "NOX",
    "SO2",
    "NO2",
    "CO2EQ",
    "VOC",
    "PM10",
    "PM25",
    "BENZ",
    "NAPTH",
    "BUTA",
    "FORM",
    "ACTE",
    "ACROL",
    "ETYB",
    "DPM",
    "POM",
]
AVG_SPEED_LIST = [2.5] + list(range(3, 76))

if __name__ == "__main__":
    path_aus_sum_2023_running_out = os.path.join(
        PATH_PROCESSED, "aus_sum_2023_running_erlt",
        "aus_sum_2023_running_erlt.xlsx")
    conn = connect_to_server_db(database_nm="aus_sum_2022_24")
    cur = conn.cursor()
    DISTRICTS_ALL = (
        "Austin",
    )
    DISTRICTS_PRCSD = DISTRICTS_ALL
    if len(DISTRICTS_PRCSD) == 1:
        DISTRICTS_PRCSD_TP = list(DISTRICTS_PRCSD)
        DISTRICTS_PRCSD_TP.append("HACK_FOR_WHERE_SQL")
        DISTRICTS_PRCSD_SQL_SAFE = tuple(DISTRICTS_PRCSD_TP)
    else:
        DISTRICTS_PRCSD_SQL_SAFE = DISTRICTS_PRCSD
    erlt_df_2014b_py = pd.read_sql(
        f"""SELECT * FROM aus_sum_2022_24_running_erlt 
        WHERE Area IN {DISTRICTS_PRCSD_SQL_SAFE}; """,
        conn,
    )
    conn.close()
    # Year Interpolation
    ############################################################################
    # Get the linearly interpolated values for interpol_vals: year 2022 to 2024
    erlt_df_2014b_py_yr_iterpolated = out_yr_spd_interpolated(
        intermediate_data=erlt_df_2014b_py,
        pollutant_cols=POLLUTANT_COLS,
        interpolation_col_dict={
            "interpol_col": "yearid",
            "interpol_vals": list(np.arange(2022, 2025, 1)),
            "grpby_cols": ["Area", "monthid", "hourid", "funclass", "avgspeed"],
        },
    ).reset_index(drop=True)
    # Output to database data with linearly interpolated values for
    # interpol_vals: year 2022 to 2024
    engine = get_engine_to_output_to_db(out_database="aus_sum_2022_24")
    erlt_df_2014b_py_yr_iterpolated.to_sql(
        "aus_sum_2022_24_running_erlt_yr_interpolated",
        con=engine,
        if_exists="replace",
        index=False,
        chunksize=10000,
    )
    # Speed Interpolation---Uses inverse of speed.
    ############################################################################
    # Interpolate avg speeds, so we have values for the following speeds: 2.5,
    # 3, 4, 5, 6...75
    erlt_df_2014b_py_yr_iterpolated_spd_interpolated = out_yr_spd_interpolated(
        intermediate_data=erlt_df_2014b_py_yr_iterpolated,
        pollutant_cols=POLLUTANT_COLS,
        interpolation_col_dict={
            "interpol_col": "avgspeed",
            "interpol_vals": AVG_SPEED_LIST,
            "grpby_cols": ["Area", "yearid", "monthid", "hourid", "funclass"],
        },
    )
    # Output speed interpolated-year interpolated table to database.
    engine = get_engine_to_output_to_db(out_database="aus_sum_2022_24")
    erlt_df_2014b_py_yr_iterpolated_spd_interpolated.to_sql(
        "aus_sum_2022_24_running_erlt_yr_spd_interpolated",
        con=engine,
        if_exists="replace",
        index=False,
        chunksize=10000,
    )

    aus_sum_2023_running_erlt_yr_spd_interpolated = (
        erlt_df_2014b_py_yr_iterpolated_spd_interpolated
        .loc[lambda df: df.yearid == 2023]
    )

    aus_sum_2023_running_erlt_yr_spd_interpolated.to_excel(
        path_aus_sum_2023_running_out)

    #QAQC
    ############################################################################
    conn = connect_to_server_db(database_nm="mvs2014b_erlt_out")
    erlt_aus = pd.read_sql(
        "SELECT * FROM running_erlt_intermediate_yr_interpolated "
        "WHERE Area = 'Austin' AND yearid = 2023 AND monthid = 7", conn)
    conn.close()
    erlt_aus_fil = (
        erlt_aus
        .filter(items=[
            'Area', 'yearid', 'monthid', 'funclass', 'avgspeed', 'POM', 'NOX',
            'CO2EQ', 'DPM'
        ])
    )

    conn = connect_to_server_db(database_nm="vmtmix_fy20")
    hourmix_aux = pd.read_sql(
        "SELECT * FROM hourmix WHERE District = 'Austin'", conn)
    hourmix_aux_1 = (
        hourmix_aux
        .rename(columns={"TOD": "hourid", "Factor": "hourmix"})
        .filter(items=["hourid", "hourmix"])
    )
    conn.close()

    aus_sum_2023_running_erlt_yr_spd_interpolated_hour_agg = (
        aus_sum_2023_running_erlt_yr_spd_interpolated
        .loc[lambda df: df.avgspeed.isin(erlt_aus_fil.avgspeed.unique())]
        .merge(
            hourmix_aux_1,
            on="hourid",
        )
        .assign(
            POM=lambda df: df.POM * df.hourmix,
            NOX=lambda df: df.NOX * df.hourmix,
            CO2EQ=lambda df: df.CO2EQ * df.hourmix,
            DPM=lambda df: df.DPM * df.hourmix,
        )
        .groupby(['Area', 'yearid', 'monthid', 'funclass', 'avgspeed'])
        .agg(
            POM=("POM", "sum"),
            NOX=("NOX", "sum"),
            CO2EQ=("CO2EQ", "sum"),
            DPM=("DPM", "sum"),
        )
        .reset_index()
    )

    pd.testing.assert_frame_equal(
        erlt_aus_fil, aus_sum_2023_running_erlt_yr_spd_interpolated_hour_agg)


