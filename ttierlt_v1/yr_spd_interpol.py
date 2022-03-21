"""
Interpolate emission rate for intermediate (odd) years or speeds.
Created by: Apoorba Bibeka
Date Created: 01/27/2021
"""
from scipy.interpolate import interp1d
import pandas as pd
import numpy as np
import os
from ttierlt_v1.utils import (
    connect_to_server_db,
    get_engine_to_output_to_db,
    PATH_INTERIM_RUNNING,
)

# TODO: Create a module based on the code before and use in for all the processes.

YEAR_LIST = np.arange(2020, 2051, 1)
AVG_SPEED_LIST = [2.5] + list(range(3, 76))
POLLUTANT_COLS = (
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
)


def pivot_df_reindex_for_qaqc(
    data,
    pivot_index,
    pivot_column="yearid",
    pollutant_cols=POLLUTANT_COLS,
):
    """
    Take the dataframes created from the tables similar to
    mvs2014b_erlt_out.running_erlt_intermediate and pivot them to allow
    QAQC operations in excel.
    Parameters
    ----------
    data: Data to pivot
    pivot_index: Columns that are not rotated
    pivot_column: Columns who's values are tranposed to create new multi-index columns.
    pollutant_cols: Columns with emission rates by pollutants. Used as the value columns
    in the pivoting operation.
    Returns
    -------
        pd.DataFrame()
            Pivoted dataframe.
    """
    index_list_dict = {
        "yearid": YEAR_LIST,
        "avgspeed": AVG_SPEED_LIST,
    }
    data_qaqc_excel_format = data.pivot_table(
        index=pivot_index,
        columns=pivot_column,
        values=pollutant_cols,
    )
    mux_pol_pivot_col = pd.MultiIndex.from_product(
        [pollutant_cols, index_list_dict[pivot_column]],
        names=["pollutants", pivot_column],
    )
    return data_qaqc_excel_format.reindex(mux_pol_pivot_col, axis=1)


def out_yr_spd_interpolated(
    intermediate_data, interpolation_col_dict, pollutant_cols=POLLUTANT_COLS
):
    """
    Create empty rows in intermediate_data based on the interpolation_col_dict.
    interpolation_col_dict can have the following two forms.
    {
            "interpol_col": "yearid",
            "interpol_vals":list(np.arange(2020, 2051, 1)),
            "grpby_cols": ["Area", "monthid", "funclass", "avgspeed"]}
    {
            "interpol_col": "avgspeed",
            "interpol_vals": AVG_SPEED_LIST,
            "grpby_cols": ["Area", "yearid", "monthid", "funclass"]}
    interpol_col: Column that forms the x-value in the interpolation: in above case it
    the yearid or the avgspeed column.
    interpol_vals: Lists all the values that the interpol_col should have.  It has the
    values with the data and the values with missing data.
    grpby_cols: These columns form groups in which we would apply the interpolation
    operation.
    Parameters
    ----------
    intermediate_data: pd.DataFrame()
        Dataframes created from the tables similar to
        mvs2014b_erlt_out.running_erlt_intermediate.
    interpolation_col_dict: dict
        dict with interpolation parameters.
    pollutant_cols: list
        List of columns names for pollutants.
    Returns
    -------
    pd.DataFrame()
        Dataframe with interpolated values.
    """
    # Iterate over partitioned data based on ["Area", "monthid", "funclass", "avgspeed"]
    # and interpolate the emission
    # rate for odd years.
    interpol_col = interpolation_col_dict["interpol_col"]
    interpol_vals = interpolation_col_dict["interpol_vals"]
    grpby_cols = interpolation_col_dict["grpby_cols"]

    intermediate_data_grps = intermediate_data.groupby(grpby_cols)
    list_interpolated_df = []
    for grp_key, grp_df in intermediate_data_grps:
        # Use multi-index to create placeholder rows in grp_df_yr_all_yr for missing
        # year.
        mux_group = pd.MultiIndex.from_product(
            [[key] for key in grp_key] + [interpol_vals],
            names=grpby_cols + [interpol_col],
        )
        set_index_list = grpby_cols + [interpol_col]
        grp_df_interpolate_fill = (
            grp_df.set_index(set_index_list).reindex(mux_group).reset_index()
        )
        if interpol_col == "yearid":
            x_interpol = grp_df.yearid.values
            x_interpol_col = "yearid"
        elif interpol_col == "avgspeed":
            # Use the inverse of average speed for interpolation.
            x_interpol = (1 / grp_df.avgspeed).values
            grp_df_interpolate_fill["inverse_avgspeed"] = (
                1 / grp_df_interpolate_fill.avgspeed
            )
            x_interpol_col = "inverse_avgspeed"
        else:
            raise ValueError(
                "interpolation_col_dict['interpol_col'] only handles "
                "yearid and avgspeed. Explicitly code how to handle other "
                "variables before using this function on other variables."
            )
        # Iterate over each pollutant---create interpolation function---fill all value
        # from the interpolation function.
        for pollutant in pollutant_cols:
            y_emisrate = grp_df[pollutant].values
            func_interpolate = interp1d(x_interpol, y_emisrate)
            assert np.allclose(
                y_emisrate, func_interpolate(x_interpol)
            ), "Interpolated value not matching given value."
            grp_df_interpolate_fill[pollutant] = func_interpolate(
                grp_df_interpolate_fill[x_interpol_col].values
            )
        list_interpolated_df.append(grp_df_interpolate_fill)
    fin_interpolated_df = pd.concat(list_interpolated_df)
    if interpol_col == "avgspeed":
        fin_interpolated_df.drop(columns="inverse_avgspeed", inplace=True)
    return fin_interpolated_df


def agg_rates_over_yr(
    data,
    grpby_cols=("Area", "yearid", "funclass", "avgspeed"),
    pollutant_cols=POLLUTANT_COLS,
    agg_func="max",
):
    """Collapse the data across months using max aggregation."""
    max_agg_within_year = {pollutant: agg_func for pollutant in pollutant_cols}
    data_yr_agg = data.groupby(grpby_cols).agg(max_agg_within_year).reset_index()
    return data_yr_agg


if __name__ == "__main__":
    # Set in and out paths.
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
    # Filter for
    conn = connect_to_server_db(database_nm="mvs2014b_erlt_out")
    erlt_df_2014b_py = pd.read_sql(
        """SELECT * FROM running_erlt_intermediate WHERE Area = 'El Paso'; """, conn
    )
    conn.close()
    # Year Interpolation
    ###################################################################################
    qaqc_manual_yr_befor_interpol = pivot_df_reindex_for_qaqc(
        data=erlt_df_2014b_py, pivot_index=["Area", "monthid", "funclass", "avgspeed"]
    )
    qaqc_manual_yr_befor_interpol.to_excel(path_qaqc_manual_before_yr_interpol)

    erlt_df_2014b_py_yr_iterpolated = out_yr_spd_interpolated(
        intermediate_data=erlt_df_2014b_py,
        pollutant_cols=POLLUTANT_COLS,
        interpolation_col_dict={
            "interpol_col": "yearid",
            "interpol_vals": list(np.arange(2020, 2051, 1)),
            "grpby_cols": ["Area", "monthid", "funclass", "avgspeed"],
        },
    )
    # Output to database and excel.
    engine = get_engine_to_output_to_db(out_database="mvs2014b_erlt_out")
    erlt_df_2014b_py_yr_iterpolated.to_sql(
        "running_erlt_intermediate_yr_interpolated",
        con=engine,
        if_exists="replace",
        index=False,
    )
    qaqc_data_yr_interpolated = pivot_df_reindex_for_qaqc(
        data=erlt_df_2014b_py_yr_iterpolated,
        pivot_index=["Area", "monthid", "funclass", "avgspeed"],
    )
    qaqc_data_yr_interpolated.to_excel(path_qaqc_py_after_yr_interpol)
    # Speed Interpolation---Uses inverse of speed.
    ###################################################################################
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

    # Output to database and excel.
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
