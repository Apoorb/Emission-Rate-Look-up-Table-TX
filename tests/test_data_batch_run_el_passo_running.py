"""
Tests to QAQC data processing from batch_run_el_passo_running_files_01.py.
"""
import pytest
import pandas as pd
import numpy as np
import src.batch_process.batch_run_el_passo_running_files_01 as batch_running_erlt


@pytest.fixture(scope="session")
def get_erlt_2014b_data():
    conn = batch_running_erlt.connect_to_server_db(database_nm="MVS2014b_ERLT_OUT")
    cur = conn.cursor()
    erlt_elp_df_2014b = pd.read_sql("SELECT * FROM El_Paso_ERLT", conn)
    return erlt_elp_df_2014b


# Write unit tests for sub-steps

def test_64_groups_by_area_year_rdtype_in_erlt_2014b_data(get_erlt_2014b_data):
    assert get_erlt_2014b_data.groupby(['Area', 'yearid', 'funclass']).ngroups == 64


def test_years_2020_to_2050_at_2yr_in_erlt_2014b_data(get_erlt_2014b_data):
    assert set(range(2020, 2052, 2)) == set(get_erlt_2014b_data.yearid)


def test_4_funcclass_in_erlt_2014b_data(get_erlt_2014b_data):
    assert {'Rural-Arterial', 'Urban-Arterial', 'Rural-Freeway', 'Urban-Freeway'} == set(get_erlt_2014b_data.funclass)


def test_unique_values_99percent_pollutants(get_erlt_2014b_data):
    get_erlt_2014b_data.columns
    num_unique_emmision_rates_pollutants = get_erlt_2014b_data[['CO', 'NOX', 'SO2', 'NO2', 'VOC', 'CO2EQ', 'PM10', 'PM25', 'BENZ', 'NAPTH', 'BUTA', 'FORM',
       'ACTE', 'ACROL', 'ETYB', 'DPM', 'POM']].nunique().values
    assert np.quantile(num_unique_emmision_rates_pollutants, 0.99) == len(get_erlt_2014b_data)
