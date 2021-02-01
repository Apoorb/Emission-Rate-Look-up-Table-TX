"""
Interpolate emission rate for intermediate (odd) years.
Created by: Apoorba Bibeka
Date Created: 01/27/2021
"""
from scipy.interpolate import interp1d
import pandas as pd
import numpy as np
import os
from ttierlt.utils import (
    connect_to_server_db,
    get_engine_to_output_to_db,
    PATH_INTERIM_RUNNING,
)

# TODO: Create a module based on the code before and use in for all the processes.

YEAR_LIST = np.arange(2020, 2051, 1)
# Pollutant columns ['CO', 'NOX', 'SO2', 'NO2', 'VOC', 'CO2EQ', 'PM10', 'PM25', 'BENZ',
# 'NAPTH', 'BUTA', 'FORM', 'ACTE',
# 'ACROL', 'ETYB', 'DPM', 'POM']


def pivot_df_reindex_for_qaqc(df_db_format, pollutant_columns):
    """
    Take the dataframes created from the table
    mvs2014b_erlt_out.running_erlt_intermediate and pivot them to allow
    QAQC operations in excel.
    """
    db_qaqc_excel_format = df_db_format.pivot_table(
        index=["Area", "monthid", "funclass", "avgspeed"],
        columns="yearid",
        values=pollutant_columns,
    )
    mux_pol_year = pd.MultiIndex.from_product(
        [pollutant_columns, YEAR_LIST], names=["pollutants", "year"]
    )
    return db_qaqc_excel_format.reindex(mux_pol_year, axis=1)


if __name__ == "__main__":
    # Set in and out paths.
    path_to_temp_mv2014b_df = os.path.join(
        PATH_INTERIM_RUNNING, "running_erlt_intermediate.xlsx"
    )
    path_to_yr_interpolated_yr_mv2014b_df_qaqc = os.path.join(
        PATH_INTERIM_RUNNING, "running_erlt_intermediate_yr_interpolated_py_qaqc.xlsx"
    )
    # Connect to server to get running_erlt_intermediate data with data for only even
    # years between 2020 and 2050.
    conn = connect_to_server_db(database_nm="mvs2014b_erlt_out")
    erlt_df_2014b_py = pd.read_sql("SELECT * FROM running_erlt_intermediate", conn)
    conn.close()
    pollutant_columns = [
        col
        for col in erlt_df_2014b_py.columns
        if col not in ["Area", "monthid", "funclass", "avgspeed", "yearid"]
    ]
    pivot_df_reindex_for_qaqc(erlt_df_2014b_py, pollutant_columns).to_excel(
        path_to_temp_mv2014b_df
    )
    # Iterate over partitioned data based on ["Area", "monthid", "funclass", "avgspeed"]
    # and interpolate the emission
    # rate for odd years.
    erlt_df_2014b_py_grpd = erlt_df_2014b_py.groupby(
        ["Area", "monthid", "funclass", "avgspeed"]
    )
    list_yr_interpolated_df = []
    for grp_key, grp_df_yr in erlt_df_2014b_py_grpd:
        x_yr = grp_df_yr.yearid.values
        # Use multi-index to create placeholder rows in grp_df_yr_all_yr for missing
        # year.
        mux_group_yr = pd.MultiIndex.from_product(
            [[grp_key[0]], [grp_key[1]], [grp_key[2]], [grp_key[3]], YEAR_LIST],
            names=["Area", "monthid", "funclass", "avgspeed", "yearid"],
        )
        set_index_list = ["Area", "monthid", "funclass", "avgspeed", "yearid"]
        grp_df_yr_all_yr = (
            grp_df_yr.set_index(set_index_list).reindex(mux_group_yr).reset_index()
        )
        # Iterate over each pollutant---create interpolation function---fill all value
        # from the interpolation function.
        for pollutant in pollutant_columns:
            y_emisrate = grp_df_yr[pollutant].values
            func_interpolate = interp1d(x_yr, y_emisrate)
            assert np.allclose(
                y_emisrate, func_interpolate(x_yr)
            ), "Interpolated value not matching given value."
            grp_df_yr_all_yr[pollutant] = func_interpolate(
                grp_df_yr_all_yr.yearid.values
            )
        list_yr_interpolated_df.append(grp_df_yr_all_yr)
    yr_interpolated_df = pd.concat(list_yr_interpolated_df)

    # Output to database and excel.
    engine = get_engine_to_output_to_db(out_database="mvs2014b_erlt_out")
    yr_interpolated_df.to_sql(
        "running_erlt_intermediate_yr_interpolated",
        con=engine,
        if_exists="replace",
        index=False,
    )
    pivot_df_reindex_for_qaqc(yr_interpolated_df, pollutant_columns).to_excel(
        path_to_yr_interpolated_yr_mv2014b_df_qaqc
    )
