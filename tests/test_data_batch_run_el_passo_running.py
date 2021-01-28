"""
Tests to QAQC data processing from 02_batch_run_el_passo_running_files.py.
"""
import pytest
import re
import pandas as pd
import numpy as np
from ttierlt.utils import connect_to_server_db


@pytest.fixture(scope="session")
def get_erlt_running_2014b_data_py(request):
    conn = connect_to_server_db(database_nm="mvs2014b_erlt_out")
    erlt_df_2014b_py = pd.read_sql(f"SELECT * FROM {request.param}", conn)
    conn.close()
    return erlt_df_2014b_py


@pytest.fixture(scope="session")
def get_qaqc_tables_created_from_sql():
    conn = connect_to_server_db(database_nm="mvs2014b_erlt_qaqc")
    cur = conn.cursor()
    cur.execute(" SHOW TABLES")
    tables_in_qaqc = cur.fetchall()
    search_pat = re.compile(".*qaqc_from_orignal$")
    table_names = [
        table[0] for table in tables_in_qaqc if re.search(search_pat, table[0])
    ]
    erlt_df_subsets_sql = pd.DataFrame()
    list_qaqc_dfs = []
    for table_nm in table_names:
        list_qaqc_dfs.append(pd.read_sql(f"SELECT * FROM {table_nm}", conn))
    erlt_df_subsets_sql = pd.concat(list_qaqc_dfs)
    return erlt_df_subsets_sql
    conn.close()


def get_erlt_running_2014b_data_py_non_fixture():
    conn = connect_to_server_db(database_nm="mvs2014b_erlt_out")
    erlt_df_2014b_py = pd.read_sql("SELECT * FROM running_erlt_intermediate", conn)
    conn.close()
    return erlt_df_2014b_py


def get_qaqc_tables_created_from_sql_non_fixture():
    conn = connect_to_server_db(database_nm="mvs2014b_erlt_qaqc")
    cur = conn.cursor()
    cur.execute(" SHOW TABLES")
    tables_in_qaqc = cur.fetchall()
    search_pat = re.compile(".*qaqc_from_orignal$")
    table_names = [
        table[0] for table in tables_in_qaqc if re.search(search_pat, table[0])
    ]
    erlt_df_subsets_sql = pd.DataFrame()
    list_qaqc_dfs = []
    for table_nm in table_names:
        list_qaqc_dfs.append(pd.read_sql(f"SELECT * FROM {table_nm}", conn))
    erlt_df_subsets_sql = pd.concat(list_qaqc_dfs)
    return erlt_df_subsets_sql
    conn.close()


def get_py_sql_df_list(df_py, df_sql):
    group_area_yearId_monthid_py = df_py.groupby(["Area", "yearid", "monthid"])
    group_area_yearId_monthid_sql = df_sql.groupby(["Area", "yearid", "monthid"])
    sql_py_df_list = []
    for sql_group_key, sql_erlt_df in group_area_yearId_monthid_sql:
        sql_group_key_nm = (
            str(sql_group_key[0])
            + "_"
            + str(sql_group_key[1])
            + "_"
            + str(sql_group_key[2])
        )
        py_erlt_df_v1 = group_area_yearId_monthid_py.get_group(
            sql_group_key
        ).reset_index(drop=True)
        sql_erlt_df_v1 = sql_erlt_df.reset_index(drop=True)
        sql_py_df_list.append((sql_erlt_df_v1, py_erlt_df_v1, sql_group_key_nm))
    return sql_py_df_list


@pytest.mark.parametrize(
    "sql_df, py_df, sql_group_key_nm",
    get_py_sql_df_list(
        get_erlt_running_2014b_data_py_non_fixture(),
        get_qaqc_tables_created_from_sql_non_fixture(),
    ),
)
def test_final_running_erlt_matches_between_py_sql_v1(sql_df, py_df, sql_group_key_nm):
    print(sql_group_key_nm)
    pd.testing.assert_frame_equal(sql_df, py_df)


@pytest.mark.parametrize(
    "get_erlt_running_2014b_data_py", ["running_erlt_intermediate"], indirect=True
)
def test_unique_groups_by_area_year_rdtype_in_erlt_2014b_data(
    get_erlt_running_2014b_data_py,
):
    assert (
        get_erlt_running_2014b_data_py.groupby(["Area", "yearid", "funclass"]).ngroups
        == 64
    )


@pytest.mark.parametrize(
    "get_erlt_running_2014b_data_py", ["running_erlt_intermediate"], indirect=True
)
def test_unique_avg_speed_2_5_to_75(get_erlt_running_2014b_data_py):
    assert all(
        get_erlt_running_2014b_data_py.groupby(
            ["Area", "yearid", "monthid", "funclass"]
        ).avgspeed.count()
        == 16
    )
    assert all(
        get_erlt_running_2014b_data_py.groupby(
            ["Area", "yearid", "monthid", "funclass"]
        ).avgspeed.nunique()
        == 16
    )
    assert set([2.5] + list(range(5, 80, 5))) == set(
        get_erlt_running_2014b_data_py.avgspeed
    )


@pytest.mark.parametrize(
    "get_erlt_running_2014b_data_py", ["running_erlt_intermediate"], indirect=True
)
def test_unique_funclass(get_erlt_running_2014b_data_py):
    assert all(
        get_erlt_running_2014b_data_py.groupby(
            ["Area", "yearid", "monthid", "avgspeed"]
        ).funclass.count()
        == 4
    )
    assert all(
        get_erlt_running_2014b_data_py.groupby(
            ["Area", "yearid", "monthid", "avgspeed"]
        ).funclass.nunique()
        == 4
    )
    assert {
        "Rural-Arterial",
        "Urban-Arterial",
        "Rural-Freeway",
        "Urban-Freeway",
    } == set(get_erlt_running_2014b_data_py.funclass)


@pytest.mark.parametrize(
    "get_erlt_running_2014b_data_py", ["running_erlt_intermediate"], indirect=True
)
def test_unique_yearid(get_erlt_running_2014b_data_py):
    assert all(
        get_erlt_running_2014b_data_py.groupby(
            ["Area", "monthid", "funclass", "avgspeed"]
        ).yearid.count()
        == 16
    )
    assert all(
        get_erlt_running_2014b_data_py.groupby(
            ["Area", "monthid", "funclass", "avgspeed"]
        ).yearid.nunique()
        == 16
    )
    assert set(range(2020, 2052, 2)) == set(get_erlt_running_2014b_data_py.yearid)


@pytest.mark.parametrize(
    "get_erlt_running_2014b_data_py", ["running_erlt_intermediate"], indirect=True
)
def test_unique_monthid(get_erlt_running_2014b_data_py):
    assert all(
        get_erlt_running_2014b_data_py.groupby(
            ["Area", "yearid", "funclass", "avgspeed"]
        ).monthid.count()
        == 4
    )
    assert all(
        get_erlt_running_2014b_data_py.groupby(
            ["Area", "yearid", "funclass", "avgspeed"]
        ).monthid.nunique()
        == 4
    )
    assert set([1, 4, 7, 10]) == set(get_erlt_running_2014b_data_py.monthid)


@pytest.mark.parametrize(
    "get_erlt_running_2014b_data_py, quantile_unique",
    [
        ("running_erlt_intermediate", 1),
        ("running_erlt_intermediate_yr_interpolated", 1),
        ("running_erlt_intermediate_yr_interpolated_spd_interpolated", 1),
    ],
    indirect=["get_erlt_running_2014b_data_py"],
)
def test_unique_values_percent_unique_pollutants(
    get_erlt_running_2014b_data_py, quantile_unique
):
    num_unique_emmision_rates_pollutants = (
        get_erlt_running_2014b_data_py[
            [
                "CO",
                "NOX",
                "SO2",
                "NO2",
                "VOC",
                "CO2EQ",
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
        ]
        .nunique()
        .values
    )
    assert np.quantile(num_unique_emmision_rates_pollutants, quantile_unique) == len(
        get_erlt_running_2014b_data_py
    )
