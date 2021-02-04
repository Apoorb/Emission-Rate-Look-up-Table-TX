"""
Tests to QAQC data processing from 01_batch_run_idling.py.
"""
import pytest
import re
import pandas as pd
import numpy as np
from ttierlt.utils import connect_to_server_db

PERIODIDS = ["AM", "MD", "PM", "ON"]
MONTHIDS = [1, 4, 7, 10]
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
DISTRICTS_ALL = [
    "El Paso",
    "Austin",
    "Corpus Christi",
    "Beaumont",
    "Dallas",
    "Fort Worth",
    "Houston",
    "Waco",
    "San Antonio",
]
DISTRICTS_PRCSD = DISTRICTS_ALL
IDLING_OUTPUT_DATASETS = [
    "idling_erlt_intermediate",
    "idling_erlt_intermediate_yr_interpolated_no_monthid",
]


@pytest.fixture(scope="session")
def get_erlt_idling_2014b_data_py(request):
    conn = connect_to_server_db(database_nm="mvs2014b_erlt_out")
    erlt_df_2014b_py = pd.read_sql(f"SELECT * FROM {request.param['data']}", conn)
    erlt_df_2014b_py_fil = erlt_df_2014b_py.loc[
        lambda df: df.Area.isin(request.param["fil_county"])
    ]
    conn.close()
    return erlt_df_2014b_py_fil


@pytest.fixture(scope="session")
def get_qaqc_tables_created_from_sql():
    """QAQC tables are created by running the original SQL scripts created
    independently of the python code."""
    conn = connect_to_server_db(database_nm="mvs2014b_erlt_qaqc")
    cur = conn.cursor()
    cur.execute(" SHOW TABLES")
    tables_in_qaqc = cur.fetchall()
    search_pat = re.compile(".*idling_qaqc_from_orignal$")
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
def get_py_sql_df_list(
    get_erlt_idling_2014b_data_py, get_qaqc_tables_created_from_sql, request
):
    group_area_yearid_monthid_py = get_erlt_idling_2014b_data_py.groupby(
        ["Area", "yearid"]
    )
    group_area_yearid_monthid_sql = get_qaqc_tables_created_from_sql.groupby(
        ["Area", "yearid"]
    )
    try:
        py_erlt_df_fil = group_area_yearid_monthid_py.get_group(
            request.param["grp_key"]
        ).reset_index(drop=True)
    except KeyError as err1:
        print(
            err1,
            f"!!!Data for {request.param['grp_key']} does not exists in "
            f"idling_erlt_intermediate!!!!",
        )
        raise
    try:
        sql_erlt_df_fil = group_area_yearid_monthid_sql.get_group(
            request.param["grp_key"]
        ).reset_index(drop=True)
    except KeyError as err1:
        print(
            err1,
            f"!!!Data for {request.param['grp_key']} does not exists in "
            f"mvs2014b_erlt_qaqc!!!!",
        )
        raise
    return {
        "py_erlt_df_fil": py_erlt_df_fil,
        "sql_erlt_df_fil": sql_erlt_df_fil,
        "grp_key": request.param["grp_key"],
    }


@pytest.mark.parametrize(
    "get_erlt_idling_2014b_data_py, get_py_sql_df_list",
    [
        (
            {"data": "idling_erlt_intermediate", "fil_county": ["El Paso"]},
            {"grp_key": ("El Paso", 2020)},
        ),
        (
            {"data": "idling_erlt_intermediate", "fil_county": ["El Paso"]},
            {"grp_key": ("El Paso", 2022)},
        ),
        (
            {"data": "idling_erlt_intermediate", "fil_county": ["El Paso"]},
            {"grp_key": ("El Paso", 2024)},
        ),
        (
            {"data": "idling_erlt_intermediate", "fil_county": ["El Paso"]},
            {"grp_key": ("El Paso", 2044)},
        ),
        (
            {"data": "idling_erlt_intermediate", "fil_county": ["Austin"]},
            {"grp_key": ("Austin", 2020)},
        ),
    ],
    ids=[
        "_".join(map(str, ("El Paso", 2020))),
        "_".join(map(str, ("El Paso", 2022))),
        "_".join(map(str, ("El Paso", 2024))),
        "_".join(map(str, ("El Paso", 2044))),
        "_".join(map(str, ("Austin", 2020))),
    ],
    indirect=True,
)
def test_final_idling_erlt_matches_between_py_sql_v1(
    get_erlt_idling_2014b_data_py, get_py_sql_df_list
):
    print(get_py_sql_df_list["grp_key"])
    pd.testing.assert_frame_equal(
        get_py_sql_df_list["py_erlt_df_fil"],
        get_py_sql_df_list["sql_erlt_df_fil"],
    )


@pytest.mark.parametrize(
    "get_erlt_idling_2014b_data_py",
    [
        {"data": "idling_erlt_intermediate", "fil_county": [district]}
        for district in DISTRICTS_PRCSD
    ],
    ids=[district for district in DISTRICTS_PRCSD],
    indirect=True,
)
def test_unique_groups_by_area_year_rdtype_in_erlt_2014b_data(
    get_erlt_idling_2014b_data_py,
):
    assert get_erlt_idling_2014b_data_py.groupby(["Area", "yearid"]).ngroups == 16
    # 4 months * 4 periods


@pytest.mark.parametrize(
    "get_erlt_idling_2014b_data_py",
    [
        {"data": "idling_erlt_intermediate", "fil_county": [district]}
        for district in DISTRICTS_PRCSD
    ],
    ids=[district for district in DISTRICTS_PRCSD],
    indirect=True,
)
def test_unique_yearid(get_erlt_idling_2014b_data_py):
    assert all(
        get_erlt_idling_2014b_data_py.groupby(
            ["Area", "monthid", "period"]
        ).yearid.count()
        == 16
    )
    assert all(
        get_erlt_idling_2014b_data_py.groupby(
            ["Area", "monthid", "period"]
        ).yearid.nunique()
        == 16
    )
    assert set(range(2020, 2052, 2)) == set(get_erlt_idling_2014b_data_py.yearid)


@pytest.mark.parametrize(
    "get_erlt_idling_2014b_data_py",
    [
        {"data": "idling_erlt_intermediate", "fil_county": [district]}
        for district in DISTRICTS_PRCSD
    ],
    ids=[district for district in DISTRICTS_PRCSD],
    indirect=True,
)
def test_unique_monthid(get_erlt_idling_2014b_data_py):
    assert all(
        get_erlt_idling_2014b_data_py.groupby(
            ["Area", "yearid", "period"]
        ).monthid.count()
        == 4
    )
    assert all(
        get_erlt_idling_2014b_data_py.groupby(
            ["Area", "yearid", "period"]
        ).monthid.nunique()
        == 4
    )
    assert set([1, 4, 7, 10]) == set(get_erlt_idling_2014b_data_py.monthid)


@pytest.mark.parametrize(
    "get_erlt_idling_2014b_data_py",
    [
        {"data": "idling_erlt_intermediate", "fil_county": [district]}
        for district in DISTRICTS_PRCSD
    ],
    ids=[district for district in DISTRICTS_PRCSD],
    indirect=True,
)
def test_unique_periods(get_erlt_idling_2014b_data_py):
    assert all(
        get_erlt_idling_2014b_data_py.groupby(
            ["Area", "yearid", "monthid"]
        ).period.count()
        == 4
    )
    assert all(
        get_erlt_idling_2014b_data_py.groupby(
            ["Area", "yearid", "monthid"]
        ).period.nunique()
        == 4
    )
    assert set(PERIODIDS) == set(get_erlt_idling_2014b_data_py.period)


@pytest.mark.parametrize(
    "get_erlt_idling_2014b_data_py, quantile_unique",
    [
        ({"data": data, "fil_county": [district]}, 0.90)
        for district in DISTRICTS_PRCSD
        for data in IDLING_OUTPUT_DATASETS
    ],
    ids=[
        "--".join([data, district])
        for district in DISTRICTS_PRCSD
        for data in IDLING_OUTPUT_DATASETS
    ],
    indirect=["get_erlt_idling_2014b_data_py"],
)
def test_some_pollutants_unique(get_erlt_idling_2014b_data_py, quantile_unique):
    # PM2.5, PM 10, DPM only have 170, 170 64 unqiue values in
    # idling_erlt_intermediatee but other pollutants seem
    # to have unique emission rates implying that the processing was okay.
    # Only check pollutants that are likely to have unique emission rates.
    num_unique_emmision_rates_pollutants = (
        get_erlt_idling_2014b_data_py[
            [col for col in ["NOX", "NO2", "VOC", "BENZ", "NAPTH", "POM"]]
        ]
        .nunique()
        .values
    )
    no_na_values = not any(np.ravel(get_erlt_idling_2014b_data_py.isna().values))
    no_empty_datasets = (len(get_erlt_idling_2014b_data_py)) > 0
    assert no_empty_datasets
    assert no_na_values
    assert all(
        num_unique_emmision_rates_pollutants
        >= len(get_erlt_idling_2014b_data_py) * quantile_unique
    )


@pytest.mark.parametrize(
    "get_erlt_idling_2014b_data_py, min_val",
    [
        ({"data": data, "fil_county": [district]}, 1)
        for district in DISTRICTS_PRCSD
        for data in IDLING_OUTPUT_DATASETS
    ],
    ids=[
        "--".join([data, district])
        for district in DISTRICTS_PRCSD
        for data in IDLING_OUTPUT_DATASETS
    ],
    indirect=["get_erlt_idling_2014b_data_py"],
)
def test_min_values_over_zero_pollutants(get_erlt_idling_2014b_data_py, min_val):
    assert all(get_erlt_idling_2014b_data_py[POLLUTANT_COLS].min() >= 0)


@pytest.mark.parametrize(
    "get_erlt_idling_2014b_data_py",
    [
        {
            "data": "idling_erlt_intermediate_yr_interpolated_no_monthid",
            "fil_county": [district],
        }
        for district in DISTRICTS_PRCSD
    ],
    ids=[district for district in DISTRICTS_PRCSD],
    indirect=True,
)
def test_correct_num_val_in_final_df(get_erlt_idling_2014b_data_py):
    assert get_erlt_idling_2014b_data_py.groupby(["Area", "yearid"]).ngroups == (
        2050 - 2020 + 1
    )
