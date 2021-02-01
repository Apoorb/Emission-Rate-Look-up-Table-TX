"""
Interpolate emission rate for intermediate speeds using the MOBILE 6.2 interpolation
formula. It uses inverse of the speed to interpolate. Take the maximum emission rate
between the four sessons as the emission rate for the year.
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

YEAR_LIST = np.arange(2020, 2051, 1)
# Pollutant columns ['CO', 'NOX', 'SO2', 'NO2', 'VOC', 'CO2EQ', 'PM10', 'PM25', 'BENZ',
# 'NAPTH', 'BUTA', 'FORM', 'ACTE',
# 'ACROL', 'ETYB', 'DPM', 'POM']
AVG_SPEED_LIST = [2.5] + list(range(3, 76))
INVERSE_AVG_SPEED_LIST = [1 / avgspeed for avgspeed in AVG_SPEED_LIST]
MAP_INVERSE_AVG_SPD = dict(zip(INVERSE_AVG_SPEED_LIST, AVG_SPEED_LIST))


def pivot_df_reindex_spd_for_qaqc(df_db_format, pollutant_columns):
    """
    Take the dataframes created from the table
    mvs2014b_erlt_out.running_erlt_intermediate and pivot them to allow
    QAQC operations in excel.
    """
    db_qaqc_excel_format = df_db_format.pivot_table(
        index=["Area", "yearid", "monthid", "funclass"],
        columns="avgspeed",
        values=pollutant_columns,
    )
    mux_pol_year = pd.MultiIndex.from_product(
        [pollutant_columns, AVG_SPEED_LIST], names=["pollutants", "year"]
    )
    return db_qaqc_excel_format.reindex(mux_pol_year, axis=1)


if __name__ == "__main__":
    # Set in and out paths.
    path_to_temp_mv2014b_df_spd = os.path.join(
        PATH_INTERIM_RUNNING,
        "running_erlt_intermediate_yr_interpolated_spd_manual.xlsx",
    )
    path_to_yr_interpolated_yr_interpolated_spd_mv2014b_df_qaqc = os.path.join(
        PATH_INTERIM_RUNNING,
        "running_erlt_intermediate_yr_interpolate_spd_interpolated_py_qaqc.xlsx",
    )
    # Connect to server to get running_erlt_intermediate data with data for only even
    # years between 2020 and 2050.
    conn = connect_to_server_db(database_nm="mvs2014b_erlt_out")
    erlt_df_2014b_yr_interpolated_py = pd.read_sql(
        "SELECT * FROM running_erlt_intermediate_yr_interpolated", conn
    )
    conn.close()

    pollutant_columns = [
        col
        for col in erlt_df_2014b_yr_interpolated_py.columns
        if col not in ["Area", "monthid", "funclass", "avgspeed", "yearid"]
    ]
    pivot_df_reindex_spd_for_qaqc(
        erlt_df_2014b_yr_interpolated_py, pollutant_columns
    ).to_excel(path_to_temp_mv2014b_df_spd)
    # Iterate over partitioned data based on ["Area", "monthid", "funclass", "avgspeed"]
    # and interpolate the emission
    # rate for odd years.
    erlt_df_2014b_py_grpd = erlt_df_2014b_yr_interpolated_py.groupby(
        ["Area", "yearid", "monthid", "funclass"]
    )
    list_spd_interpolated_df = []
    for grp_key, grp_df_spd in erlt_df_2014b_py_grpd:
        # Use multi-index to create placeholder rows in grp_df_yr_all_yr for missing
        # year.
        set_index_list = ["Area", "yearid", "monthid", "funclass", "avgspeed"]
        mux_group_avgspd = pd.MultiIndex.from_product(
            [[grp_key[0]], [grp_key[1]], [grp_key[2]], [grp_key[3]], AVG_SPEED_LIST],
            names=set_index_list,
        )
        grp_df_spd_all_invverse_spds = (
            grp_df_spd.set_index(set_index_list).reindex(mux_group_avgspd).reset_index()
        )
        x_spd = (1 / grp_df_spd.avgspeed).values
        grp_df_spd_all_invverse_spds["inverse_avgspeed"] = (
            1 / grp_df_spd_all_invverse_spds.avgspeed
        )
        # Iterate over each pollutant---create interpolation function---fill all value
        # from the interpolation function.
        for pollutant in pollutant_columns:
            y_emisrate = grp_df_spd[pollutant].values
            func_interpolate = interp1d(x_spd, y_emisrate)
            assert np.allclose(
                y_emisrate, func_interpolate(x_spd)
            ), "Interpolated value not matching given value."
            grp_df_spd_all_invverse_spds[pollutant] = func_interpolate(
                grp_df_spd_all_invverse_spds.inverse_avgspeed.values
            )
        list_spd_interpolated_df.append(grp_df_spd_all_invverse_spds)
    spd_interpolated_df = pd.concat(list_spd_interpolated_df)
    spd_interpolated_df.drop(columns="inverse_avgspeed", inplace=True)

    # Collapse the data across months using max aggregation.
    max_agg_within_year = {pollutant: "max" for pollutant in pollutant_columns}
    spd_interpolated_df_yr_agg = (
        spd_interpolated_df.groupby(["Area", "yearid", "funclass", "avgspeed"])
        .agg(max_agg_within_year)
        .reset_index()
    )

    # Output to database and excel.
    engine = get_engine_to_output_to_db(out_database="mvs2014b_erlt_out")
    spd_interpolated_df_yr_agg.to_sql(
        "running_erlt_intermediate_yr_spd_interpolated_no_monthid",
        con=engine,
        if_exists="replace",
        index=False,
    )
    pivot_df_reindex_spd_for_qaqc(spd_interpolated_df, pollutant_columns).to_excel(
        path_to_yr_interpolated_yr_interpolated_spd_mv2014b_df_qaqc
    )
