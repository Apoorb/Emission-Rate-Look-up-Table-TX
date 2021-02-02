"""
Interpolate emission rate for intermediate (odd) years.

Interpolate emission rate for intermediate speeds using the MOBILE 6.2 interpolation
formula. It uses inverse of the speed to interpolate. Take the maximum emission rate
between the four sessons as the emission rate for the year.
Created by: Apoorba Bibeka
Date Created: 01/27/2021

Created by: Apoorba Bibeka
Date Created: 01/27/2021
"""
import pandas as pd
import numpy as np
import os
from ttierlt.utils import (
    connect_to_server_db,
    get_engine_to_output_to_db,
    PATH_INTERIM_RUNNING,
)
from ttierlt.yr_spd_interpol import (
    pivot_df_reindex_for_qaqc,
    out_yr_spd_interpolated,
    agg_rates_over_yr,
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
    # Set out paths.
    path_qaqc_manual_before_yr_interpol = os.path.join(
        PATH_INTERIM_RUNNING, "qacqc_manual_yr_interpol_running_erlt_intermediate.xlsx"
    )
    path_qaqc_py_after_yr_interpol = os.path.join(
        PATH_INTERIM_RUNNING, "qacqc_py_yr_interpol_running_erlt_intermediate.xlsx"
    )
    path_qaqc_manual_before_spd_interpol = os.path.join(
        PATH_INTERIM_RUNNING,
        "qacqc_manual_spd_interpol_running_erlt_intermediate.xlsx",
    )
    path_qaqc_py_after_spd_interpol = os.path.join(
        PATH_INTERIM_RUNNING,
        "qacqc_py_spd_interpol_running_erlt_intermediate.xlsx",
    )
    # Connect to server to get running_erlt_intermediate data with data for only even
    # years between 2020 and 2050.
    # TODO: Remove the Where Area = "El Paso" filter after running all runs.
    # raise ValueError("erlt_df_2014b_py should eventually contain all districts")
    conn = connect_to_server_db(database_nm="mvs2014b_erlt_out")
    DISTRICTS_ALL = (
        "El Paso",
        "Austin",
        "Corpus Christi",
        "Beaumont",
        "Dallas",
        "Fort Worth",
        "Houston",
        "Waco",
        "San Antonio"
    )
    DISTRICTS_PRCSD = DISTRICTS_ALL[0:6]
    if len(DISTRICTS_PRCSD) == 1:
        DISTRICTS_PRCSD_TP = list(DISTRICTS_PRCSD)
        DISTRICTS_PRCSD_TP.append("HACK_FOR_WHERE_SQL")
        DISTRICTS_PRCSD_SQL_SAFE = tuple(DISTRICTS_PRCSD_TP)
    else:
        DISTRICTS_PRCSD_SQL_SAFE = DISTRICTS_PRCSD
    erlt_df_2014b_py = pd.read_sql(
        f"""SELECT * FROM running_erlt_intermediate 
        WHERE Area IN {DISTRICTS_PRCSD_SQL_SAFE}; """,
        conn
    )
    conn.close()
    # Year Interpolation
    ###################################################################################
    # Get pivot table to manually interpolate by year---emission rates in excel.
    qaqc_manual_yr_befor_interpol = pivot_df_reindex_for_qaqc(
        data=erlt_df_2014b_py, pivot_index=["Area", "monthid", "funclass", "avgspeed"]
    )
    qaqc_manual_yr_befor_interpol.to_excel(path_qaqc_manual_before_yr_interpol)
    # Get the linearly interpolated values for interpol_vals: year 2020 to 2050
    erlt_df_2014b_py_yr_iterpolated = out_yr_spd_interpolated(
        intermediate_data=erlt_df_2014b_py,
        pollutant_cols=POLLUTANT_COLS,
        interpolation_col_dict={
            "interpol_col": "yearid",
            "interpol_vals": list(np.arange(2020, 2051, 1)),
            "grpby_cols": ["Area", "monthid", "funclass", "avgspeed"],
        },
    )
    # Output to database data with linearly interpolated values for interpol_vals: year
    # 2020 to 2050
    engine = get_engine_to_output_to_db(out_database="mvs2014b_erlt_out")
    erlt_df_2014b_py_yr_iterpolated.to_sql(
        "running_erlt_intermediate_yr_interpolated",
        con=engine,
        if_exists="replace",
        index=False,
    )
    # Get pivot table of the data with linearly interpolated values for interpol_vals:
    # year 2020 to 2050.
    qaqc_data_yr_interpolated = pivot_df_reindex_for_qaqc(
        data=erlt_df_2014b_py_yr_iterpolated,
        pivot_index=["Area", "monthid", "funclass", "avgspeed"],
    )
    qaqc_data_yr_interpolated.to_excel(path_qaqc_py_after_yr_interpol)
    # Speed Interpolation---Uses inverse of speed.
    ###################################################################################
    # Get pivot table to manually interpolate by speed---emission rates in excel.
    qaqc_data_yr_iterpolated_spd_manual = pivot_df_reindex_for_qaqc(
        data=erlt_df_2014b_py_yr_iterpolated,
        pivot_index=["Area", "yearid", "monthid", "funclass"],
        pivot_column="avgspeed",
        pollutant_cols=POLLUTANT_COLS,
    )
    qaqc_data_yr_iterpolated_spd_manual.to_excel(path_qaqc_manual_before_spd_interpol)
    # Iterpolate avg speeds, so we have values for the following speeds: 2.5, 3, 4, 5,
    # 6...75
    erlt_df_2014b_py_yr_iterpolated_spd_interpolated = out_yr_spd_interpolated(
        intermediate_data=erlt_df_2014b_py_yr_iterpolated,
        pollutant_cols=POLLUTANT_COLS,
        interpolation_col_dict={
            "interpol_col": "avgspeed",
            "interpol_vals": AVG_SPEED_LIST,
            "grpby_cols": ["Area", "yearid", "monthid", "funclass"],
        },
    )
    # Remove months---Aggregate over year---take the max emission rate for the year
    # groups as the yearly emission rate.
    ###################################################################################
    erlt_df_2014b_py_yr_iterpolated_spd_interpolated_agg = agg_rates_over_yr(
        data=erlt_df_2014b_py_yr_iterpolated_spd_interpolated,
        grpby_cols=["Area", "yearid", "funclass", "avgspeed"],
        pollutant_cols=POLLUTANT_COLS,
        agg_func="max",
    )
    # Output speed interpolated-year interpolated table no month column (aggregated
    # as sum) to database and excel.
    engine = get_engine_to_output_to_db(out_database="mvs2014b_erlt_out")
    erlt_df_2014b_py_yr_iterpolated_spd_interpolated_agg.to_sql(
        "running_erlt_intermediate_yr_spd_interpolated_no_monthid",
        con=engine,
        if_exists="replace",
        index=False,
    )
    qaqc_data_yr_iterpolated_spd_interpolated = pivot_df_reindex_for_qaqc(
        data=erlt_df_2014b_py_yr_iterpolated_spd_interpolated,
        pivot_index=["Area", "yearid", "monthid", "funclass"],
        pivot_column="avgspeed",
        pollutant_cols=POLLUTANT_COLS,
    )
    qaqc_data_yr_iterpolated_spd_interpolated.to_excel(path_qaqc_py_after_spd_interpol)
