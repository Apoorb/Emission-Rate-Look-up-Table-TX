"""
Tests to QAQC data processing from 01_batch_run_starts.py.
"""
import pytest
import re
import pandas as pd
import numpy as np
from ttierlt_v1.utils import connect_to_server_db

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
VEHTYPES = [
    "Combination Long-haul Truck",
    "Combination Short-haul Truck",
    "Intercity Bus",
    "Light Commercial Truck",
    "Motor Home",
    "Motorcycle",
    "Passenger Car",
    "Passenger Truck",
    "Refuse Truck",
    "School Bus",
    "Single Unit Long-haul Truck",
    "Single Unit Short-haul Truck",
    "Transit Bus",
]
FUELTYPES = ["Gasoline", "Diesel"]
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
STARTS_OUTPUT_DATASETS = [
    "starts_erlt_intermediate",
    "starts_erlt_intermediate_yr_interpolated_no_monthid",
]


@pytest.fixture(scope="session")
def get_erlt_starts_2014b_data_py(request):
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
def get_py_sql_df_list(
    get_erlt_starts_2014b_data_py, get_qaqc_tables_created_from_sql, request
):
    group_area_yearid_monthid_py = get_erlt_starts_2014b_data_py.groupby(
        ["Area", "yearid", "monthid"]
    )
    group_area_yearid_monthid_sql = get_qaqc_tables_created_from_sql.groupby(
        ["Area", "yearid", "monthid"]
    )
    try:
        py_erlt_df_fil = group_area_yearid_monthid_py.get_group(
            request.param["grp_key"]
        ).reset_index(drop=True)
    except KeyError as err1:
        print(
            err1,
            f"!!!Data for {request.param['grp_key']} does not exists in "
            f"starts_erlt_intermediate!!!!",
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
    "get_erlt_starts_2014b_data_py, get_py_sql_df_list",
    [
        (
            {"data": "starts_erlt_intermediate", "fil_county": ["El Paso"]},
            {"grp_key": ("El Paso", 2020, 1)},
        ),
        (
            {"data": "starts_erlt_intermediate", "fil_county": ["El Paso"]},
            {"grp_key": ("El Paso", 2022, 7)},
        ),
        (
            {"data": "starts_erlt_intermediate", "fil_county": ["El Paso"]},
            {"grp_key": ("El Paso", 2024, 10)},
        ),
        (
            {"data": "starts_erlt_intermediate", "fil_county": ["El Paso"]},
            {"grp_key": ("El Paso", 2044, 4)},
        ),
        (
            {"data": "starts_erlt_intermediate", "fil_county": ["Austin"]},
            {"grp_key": ("Austin", 2020, 1)},
        ),
    ],
    ids=[
        "_".join(map(str, ("El Paso", 2020, 1))),
        "_".join(map(str, ("El Paso", 2022, 7))),
        "_".join(map(str, ("El Paso", 2024, 10))),
        "_".join(map(str, ("El Paso", 2044, 4))),
        "_".join(map(str, ("Austin", 2020, 1))),
    ],
    indirect=True,
)
def test_final_starts_erlt_matches_between_py_sql_v1(
    get_erlt_starts_2014b_data_py, get_py_sql_df_list
):
    print(get_py_sql_df_list["grp_key"])
    pd.testing.assert_frame_equal(
        get_py_sql_df_list["py_erlt_df_fil"],
        get_py_sql_df_list["sql_erlt_df_fil"],
    )


@pytest.mark.parametrize(
    "get_erlt_starts_2014b_data_py",
    [
        {"data": "starts_erlt_intermediate", "fil_county": [district]}
        for district in DISTRICTS_PRCSD
    ],
    ids=[district for district in DISTRICTS_PRCSD],
    indirect=True,
)
def test_unique_groups_by_area_year_in_erlt_2014b_data(
    get_erlt_starts_2014b_data_py,
):
    assert get_erlt_starts_2014b_data_py.groupby(["Area", "yearid"]).ngroups == 16


@pytest.mark.parametrize(
    "get_erlt_starts_2014b_data_py",
    [
        {"data": "starts_erlt_intermediate", "fil_county": [district]}
        for district in DISTRICTS_PRCSD
    ],
    ids=[district for district in DISTRICTS_PRCSD],
    indirect=True,
)
def test_unique_yearid(get_erlt_starts_2014b_data_py):
    assert set(range(2020, 2052, 2)) == set(get_erlt_starts_2014b_data_py.yearid)
    assert all(
        get_erlt_starts_2014b_data_py.groupby(
            ["Area", "monthid", "VehicleType", "FUELTYPE"]
        ).yearid.count()
        == 16
    )
    assert all(
        get_erlt_starts_2014b_data_py.groupby(
            ["Area", "monthid", "VehicleType", "FUELTYPE"]
        ).yearid.nunique()
        == 16
    )


@pytest.mark.parametrize(
    "get_erlt_starts_2014b_data_py",
    [
        {"data": "starts_erlt_intermediate", "fil_county": [district]}
        for district in DISTRICTS_PRCSD
    ],
    ids=[district for district in DISTRICTS_PRCSD],
    indirect=True,
)
def test_unique_monthid(get_erlt_starts_2014b_data_py):
    assert all(
        get_erlt_starts_2014b_data_py.groupby(
            ["Area", "yearid", "VehicleType", "FUELTYPE"]
        ).monthid.count()
        == 4
    )
    assert all(
        get_erlt_starts_2014b_data_py.groupby(
            ["Area", "yearid", "VehicleType", "FUELTYPE"]
        ).monthid.nunique()
        == 4
    )
    assert set([1, 4, 7, 10]) == set(get_erlt_starts_2014b_data_py.monthid)


@pytest.mark.parametrize(
    "get_erlt_starts_2014b_data_py",
    [
        {"data": "starts_erlt_intermediate", "fil_county": [district]}
        for district in DISTRICTS_PRCSD
    ],
    ids=[district for district in DISTRICTS_PRCSD],
    indirect=True,
)
def test_unique_vehicletypes_fueltypes(get_erlt_starts_2014b_data_py):
    assert all(
        get_erlt_starts_2014b_data_py.groupby(
            ["Area", "yearid", "monthid"]
        ).VehicleType.nunique()
        == 13
    )
    assert set(VEHTYPES) == set(get_erlt_starts_2014b_data_py.VehicleType)
    assert set(FUELTYPES) == set(get_erlt_starts_2014b_data_py.FUELTYPE)


@pytest.mark.parametrize(
    "get_erlt_starts_2014b_data_py, quantile_unique",
    [
        ({"data": data, "fil_county": [district]}, 0.95)
        for district in DISTRICTS_PRCSD
        for data in STARTS_OUTPUT_DATASETS
    ],
    ids=[
        "--".join([data, district])
        for district in DISTRICTS_PRCSD
        for data in STARTS_OUTPUT_DATASETS
    ],
    indirect=["get_erlt_starts_2014b_data_py"],
)
def test_unique_values_percent_unique_pollutants(
    get_erlt_starts_2014b_data_py, quantile_unique
):
    # Only check pollutants that show variation.
    pollutants_that_have_unique_emissions = [
        "SO2",
        "CO2EQ",
        "VOC",
        "BENZ",
        "NAPTH",
        "BUTA",
        "FORM",
        "ACTE",
        "ACROL",
        "ETYB",
        "POM",
    ]
    num_unique_emmision_rates_pollutants = (
        get_erlt_starts_2014b_data_py[pollutants_that_have_unique_emissions]
        .nunique()
        .values
    )
    # Ignore the duplicate 0s.
    expected_unique_rates_pollutants = (
        get_erlt_starts_2014b_data_py[pollutants_that_have_unique_emissions]
        .gt(0)
        .sum()
        .values
    )
    no_na_values = not any(np.ravel(get_erlt_starts_2014b_data_py.isna().values))
    no_empty_datasets = (len(get_erlt_starts_2014b_data_py)) > 0
    assert no_empty_datasets
    assert no_na_values
    assert all(
        num_unique_emmision_rates_pollutants
        >= (expected_unique_rates_pollutants * quantile_unique)
    )


@pytest.mark.parametrize(
    "get_erlt_starts_2014b_data_py, min_val",
    [
        ({"data": data, "fil_county": [district]}, 1)
        for district in DISTRICTS_PRCSD
        for data in STARTS_OUTPUT_DATASETS
    ],
    ids=[
        "--".join([data, district])
        for district in DISTRICTS_PRCSD
        for data in STARTS_OUTPUT_DATASETS
    ],
    indirect=["get_erlt_starts_2014b_data_py"],
)
def test_min_values_over_zero_pollutants(get_erlt_starts_2014b_data_py, min_val):
    assert all(get_erlt_starts_2014b_data_py[POLLUTANT_COLS].min() >= 0)


@pytest.mark.parametrize(
    "get_erlt_starts_2014b_data_py",
    [
        {
            "data": "starts_erlt_intermediate_yr_interpolated_no_monthid",
            "fil_county": [district],
        }
        for district in DISTRICTS_PRCSD
    ],
    ids=[district for district in DISTRICTS_PRCSD],
    indirect=True,
)
def test_correct_num_val_in_final_df(get_erlt_starts_2014b_data_py):
    assert (
        get_erlt_starts_2014b_data_py.groupby(
            ["Area", "yearid", "VehicleType", "FUELTYPE"]
        ).ngroups
        == (2050 - 2020 + 1) * 22
    )  # Number of unique fuel and vehicle type combo


@pytest.mark.parametrize(
    "get_erlt_starts_2014b_data_py, quantile_unique",
    [
        (
            {
                "data": "starts_erlt_intermediate_yr_interpolated_no_monthid",
                "fil_county": DISTRICTS_ALL,
            },
            0.95,
        )
    ],
    ids=["--".join(["starts final data", "all districts"])],
    indirect=["get_erlt_starts_2014b_data_py"],
)
def test_final_data_all_districts_unique_values_percent_unique_pollutants(
    get_erlt_starts_2014b_data_py, quantile_unique
):
    # Only check pollutants that show variation.
    pollutants_that_have_unique_emissions = [
        "SO2",
        "CO2EQ",
        "VOC",
        "BENZ",
        "NAPTH",
        "BUTA",
        "FORM",
        "ACTE",
        "ACROL",
        "ETYB",
        "POM",
    ]
    num_unique_emmision_rates_pollutants = (
        get_erlt_starts_2014b_data_py[pollutants_that_have_unique_emissions]
        .nunique()
        .values
    )
    # Ignore the duplicate 0s.
    expected_unique_rates_pollutants = (
        get_erlt_starts_2014b_data_py[pollutants_that_have_unique_emissions]
        .gt(0)
        .sum()
        .values
    )
    no_na_values = not any(np.ravel(get_erlt_starts_2014b_data_py.isna().values))
    no_empty_datasets = (len(get_erlt_starts_2014b_data_py)) > 0
    assert no_empty_datasets
    assert no_na_values
    assert all(
        num_unique_emmision_rates_pollutants
        >= (expected_unique_rates_pollutants * quantile_unique)
    )
