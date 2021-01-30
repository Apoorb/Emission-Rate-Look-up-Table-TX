"""
Tests to QAQC data processing from 02_batch_run_el_passo_running_files.py.
"""
import pytest
import re
import pandas as pd
import numpy as np
from ttierlt.utils import connect_to_server_db

@pytest.fixture(scope="session")
def get_pollutant_cols():
    return [
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

@pytest.fixture(scope="session")
def get_erlt_starts_2014b_data_py(request):
    conn = connect_to_server_db(database_nm="mvs2014b_erlt_out")
    erlt_df_2014b_py = pd.read_sql(f"SELECT * FROM {request.param['data']}", conn)
    erlt_df_2014b_py_fil = erlt_df_2014b_py.loc[lambda df: df.Area.isin(request.param["fil_county"])]
    conn.close()
    return erlt_df_2014b_py_fil


@pytest.fixture(scope="session")
def get_qaqc_tables_created_from_sql():
    conn = connect_to_server_db(database_nm="mvs2014b_erlt_qaqc")
    cur = conn.cursor()
    cur.execute(" SHOW TABLES")
    tables_in_qaqc = cur.fetchall()
    search_pat = re.compile(".*starts_qaqc_from_orignal$")
    table_names = [
        table[0] for table in tables_in_qaqc if re.search(search_pat, table[0])
    ]
    list_qaqc_dfs = []
    for table_nm in table_names:
        list_qaqc_dfs.append(pd.read_sql(f"SELECT * FROM {table_nm}", conn))
    erlt_df_subsets_sql = pd.concat(list_qaqc_dfs)
    return erlt_df_subsets_sql
    conn.close()


@pytest.fixture(scope="session")
def get_py_sql_df_list(get_erlt_starts_2014b_data_py, get_qaqc_tables_created_from_sql, request):
    group_area_yearid_monthid_py = get_erlt_starts_2014b_data_py.groupby(["Area", "yearid", "monthid"])
    group_area_yearid_monthid_sql = get_qaqc_tables_created_from_sql.groupby(["Area", "yearid", "monthid"])
    try:
        py_erlt_df_fil = group_area_yearid_monthid_py.get_group(request.param["grp_key"]).reset_index(drop=True)
    except KeyError as err1:
        print(err1, f"!!!Data for {request.param['grp_key']} does not exists in starts_erlt_intermediate!!!!")
        raise
    try:
        sql_erlt_df_fil = group_area_yearid_monthid_sql.get_group(request.param["grp_key"]).reset_index(drop=True)
    except KeyError as err1:
        print(err1, f"!!!Data for {request.param['grp_key']} does not exists in mvs2014b_erlt_qaqc!!!!")
        raise
    return {"py_erlt_df_fil": py_erlt_df_fil, "sql_erlt_df_fil": sql_erlt_df_fil, "grp_key": request.param["grp_key"]}


@pytest.mark.parametrize(
    "get_erlt_starts_2014b_data_py, get_py_sql_df_list",
    [
         ({"data" : "starts_erlt_intermediate", "fil_county": ["El Paso"]}, {"grp_key": ("El Paso", 2020, 1)}),
        # ({"data": "starts_erlt_intermediate", "fil_county": ["El Paso"]}, {"grp_key": ("El Paso", 2022, 7)}),
        # ({"data": "starts_erlt_intermediate", "fil_county": ["El Paso"]}, {"grp_key": ("El Paso", 2024, 10)}),
        # ({"data": "starts_erlt_intermediate", "fil_county": ["El Paso"]}, {"grp_key": ("El Paso", 2044, 4)}),
    ],
    ids=["_".join(map(str, ("El Paso", 2020, 1))),
         #"_".join(map(str, ("El Paso", 2022, 7))),
         #"_".join(map(str, ("El Paso", 2024, 10))), "_".join(map(str, ("El Paso", 2044, 4)))
         ],
    indirect=True
)
def test_final_starts_erlt_matches_between_py_sql_v1(get_erlt_starts_2014b_data_py, get_py_sql_df_list):
    print(get_py_sql_df_list["grp_key"])
    pd.testing.assert_frame_equal(get_py_sql_df_list["py_erlt_df_fil"], get_py_sql_df_list["sql_erlt_df_fil"], rtol=0.0001)

    get_erlt_running_2014b_data_py[get_pollutant_cols].min() > 0