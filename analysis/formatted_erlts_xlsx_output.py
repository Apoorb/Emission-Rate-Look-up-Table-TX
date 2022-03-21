"""
Output final formatted ERLTS.
"""

import pandas as pd
import numpy as np
import os
from ttierlt_v1.utils import (
    connect_to_server_db,
    PATH_PROCESSED_RUNNING,
    PATH_PROCESSED_STARTS,
    PATH_PROCESSED_IDLING,
    PATH_PROCESSED_EXTNIDLE,
)

if __name__ == "__main__":
    path_out_running = os.path.join(PATH_PROCESSED_RUNNING, "running_erlt_2014b.xlsx")
    path_out_start = os.path.join(PATH_PROCESSED_STARTS, "start_erlt_2014b.xlsx")
    path_out_idling = os.path.join(PATH_PROCESSED_IDLING, "idling_erlt_2014b.xlsx")
    path_out_extnidle = os.path.join(
        PATH_PROCESSED_EXTNIDLE, "extnidle_erlt_2014b.xlsx"
    )

    conn = connect_to_server_db(database_nm="mvs2014b_erlt_out")
    cur = conn.cursor()
    DISTRICTS_ALL = (
        "El Paso",
        "Austin",
        "Corpus Christi",
        "Beaumont",
        "Dallas",
        "Fort Worth",
        "Houston",
        "Waco",
        "San Antonio",
    )
    DISTRICTS_PRCSD = DISTRICTS_ALL
    if len(DISTRICTS_PRCSD) == 1:
        DISTRICTS_PRCSD_TP = list(DISTRICTS_PRCSD)
        DISTRICTS_PRCSD_TP.append("HACK_FOR_WHERE_SQL")
        DISTRICTS_PRCSD_SQL_SAFE = tuple(DISTRICTS_PRCSD_TP)
    else:
        DISTRICTS_PRCSD_SQL_SAFE = DISTRICTS_PRCSD
    running_2014b = pd.read_sql(
        f"""SELECT * FROM running_erlt_intermediate_yr_spd_interpolated_no_monthid 
        WHERE Area IN {DISTRICTS_PRCSD_SQL_SAFE}; """,
        conn,
    )

    start_2014b = pd.read_sql(
        f"""SELECT * FROM starts_erlt_intermediate_yr_interpolated_no_monthid 
        WHERE Area IN {DISTRICTS_PRCSD_SQL_SAFE}; """,
        conn,
    )

    idling_2014b = pd.read_sql(
        f"""SELECT * FROM idling_erlt_intermediate_yr_interpolated_no_monthid 
        WHERE Area IN {DISTRICTS_PRCSD_SQL_SAFE}; """,
        conn,
    )

    extnidle_2014b = pd.read_sql(
        f"""SELECT * FROM extnidle_erlt_intermediate_yr_interpolated_no_monthid 
        WHERE Area IN {DISTRICTS_PRCSD_SQL_SAFE}; """,
        conn,
    )
    conn.close()

    running_2014b.funclass.unique()

    moves_funclass_map = {
        "Rural-Arterial": "Rural Unrestricted Access",
        "Rural-Freeway": "Rural Restricted Access",
        "Urban-Arterial": "Urban Unrestricted Access",
        "Urban-Freeway": "Urban Restricted Access",
    }

    moves_rdtypeid_map = {
        "Rural Restricted Access": 2,
        "Rural Unrestricted Access": 3,
        "Urban Restricted Access": 4,
        "Urban Unrestricted Access": 5,
    }

    moves_sutid_map = {
        "Motorcycle": 11,
        "Passenger Car": 21,
        "Passenger Truck": 31,
        "Light Commercial Truck": 32,
        "Intercity Bus": 41,
        "Transit Bus": 42,
        "School Bus": 43,
        "Refuse Truck": 51,
        "Single Unit Short-haul Truck": 52,
        "Single Unit Long-haul Truck": 53,
        "Motor Home": 54,
        "Combination Short-haul Truck": 61,
        "Combination Long-haul Truck": 62,
    }

    moves_fueltyid_map = {
        "Gasoline": 1,
        "Diesel": 2,
    }

    running_2014b_1 = (
        running_2014b.assign(
            funclass=lambda df: df.funclass.map(moves_funclass_map),
            rdtypeid=lambda df: df.funclass.map(moves_rdtypeid_map),
        )
        .rename(
            columns={
                "funclass": "Road Description",
                "yearid": "Year",
                "rdtypeid": "Road Type ID",
                "avgspeed": "Average Speed",
            }
        )
        .filter(
            items=[
                "Area",
                "Year",
                "Road Type ID",
                "Road Description",
                "Average Speed",
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
        )
        .sort_values(
            by=["Area", "Year", "Road Type ID", "Road Description", "Average Speed"]
        )
    )

    start_2014b_1 = (
        start_2014b.assign(
            veh_id=lambda df: df.VehicleType.map(moves_sutid_map),
            fuel_id=lambda df: df.FUELTYPE.map(moves_fueltyid_map),
        )
        .sort_values(by=["Area", "yearid", "veh_id", "fuel_id"])
        .rename(
            columns={
                "yearid": "Year",
                "veh_id": "Source Type ID",
                "fuel_id": "Fuel Type ID",
                "VehicleType": "Source Type",
                "FUELTYPE": "Fuel Type",
            }
        )
        .filter(
            items=[
                "Area",
                "Year",
                "Source Type ID",
                "Fuel Type ID",
                "Source Type",
                "Fuel Type",
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
        )
    )

    idling_2014b_1 = idling_2014b.rename(columns={"yearid": "Year"})

    extnidle_2014b_1 = (
        extnidle_2014b.assign(
            process_id=lambda df: df.Processtype.map(
                {
                    "APU": "91",
                    "Extnd_Exhaust": "17 & 90",
                }
            ),
            Processtype=lambda df: df.Processtype.map(
                {
                    "APU": "Auxiliary Power Unit",
                    "Extnd_Exhaust": "Extended Idling on Truck Engine",
                }
            ),
        )
        .sort_values(by=["Area", "yearid", "Processtype"])
        .rename(
            columns={
                "yearid": "Year",
                "Processtype": "Process Type",
                "process_id": "Process Type ID",
            }
        )
        .filter(
            items=[
                "Area",
                "Year",
                "Process Type ID",
                "Process Type",
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
        )
    )

    with pd.ExcelWriter(path_out_running) as writer:
        for area in running_2014b_1.Area.unique():
            erlt_df_2014b_py_1_fil = running_2014b_1.loc[lambda df: df.Area == area]
            erlt_df_2014b_py_1_fil.to_excel(writer, sheet_name=area, index=False)

    with pd.ExcelWriter(path_out_start) as writer:
        for area in start_2014b_1.Area.unique():
            start_2014b_1_fil = start_2014b_1.loc[lambda df: df.Area == area]
            start_2014b_1_fil.to_excel(writer, sheet_name=area, index=False)

    with pd.ExcelWriter(path_out_idling) as writer:
        for area in idling_2014b_1.Area.unique():
            idling_2014b_1_fil = idling_2014b_1.loc[lambda df: df.Area == area]
            idling_2014b_1_fil.to_excel(writer, sheet_name=area, index=False)

    with pd.ExcelWriter(path_out_extnidle) as writer:
        for area in extnidle_2014b_1.Area.unique():
            extnidle_2014b_1_fil = extnidle_2014b_1.loc[lambda df: df.Area == area]
            extnidle_2014b_1_fil.to_excel(writer, sheet_name=area, index=False)
