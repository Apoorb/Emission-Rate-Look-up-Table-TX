"""
Process 2014 ERLTs.
"""

import pandas as pd
import numpy as np
import os
from ttierlt.utils import PATH_RAW, PATH_PROCESSED

path_plotting_df = os.path.join(PATH_PROCESSED, "erlt_2014_2014b.csv")
path_plotting_df2 = (
    r"C:\Users\a-bibeka\PycharmProjects\ERLT_Plot\data\erlt_2014_2014b.csv")
erlt_df_2014b = pd.read_csv(PATH_PROCESSED + "/running_df_final.csv")

erlt_df_2014b = (
    erlt_df_2014b.assign(
        funclass=lambda df: df.funclass.map(
            {
                "Rural-Freeway": "Rural Restricted Access",
                "Rural-Arterial": "Rural Unrestricted Access",
                "Urban-Freeway": "Urban Restricted Access",
                "Urban-Arterial": "Urban Unrestricted Access",
            }
        ),
    )
    .rename(
        columns={
            "CO2EQ": "CO2",
            "yearid": "Year",
            "funclass": "Road Description",
            "avgspeed": "Average Speed (mph)",
        }
    )
    .melt(
        id_vars=["Area", "Year", "Road Description", "Average Speed (mph)"],
        value_vars=[
            "CO",
            "NOX",
            "SO2",
            "NO2",
            "VOC",
            "CO2",
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
        ],
        var_name="Pollutant",
        value_name="2021 Emission Rates (grams/mile)",
    )
)

new_old_district_map = {
    "Austin": "Austin",
    "Corpus Christi": "Corpus",
    "Dallas": "DFW",
    "Fort Worth": "DFW",
    "El Paso": "El Paso",
    "Houston": "HGB",
    "San Antonio": "San Antonio",
    "Waco": "Waco",
}

x1 = pd.ExcelFile(PATH_RAW + "/erlt_2014_criteria_msats.xlsx")
assert set(x1.sheet_names) == set(new_old_district_map.values()), (
    "Some of the name " "in the previous " "study do not " "match."
)

erlt_dict_2014 = {}
for new_district, old_district in new_old_district_map.items():
    erlt_df_2014_tmp_1 = x1.parse(old_district)

    erlt_df_2014_tmp_2 = (
        erlt_df_2014_tmp_1.rename(columns={"Road Description": "funclass"})
        .assign(
            area_old=lambda df: df.Area,
            Area=new_district,
        )
        .drop(columns=["Road Type ID"])
        .rename(
            columns={
                "funclass": "Road Description",
                "area_old": "Area—Previous Study",
                "Average Speed": "Average Speed (mph)",
            }
        )
        .melt(
            id_vars=[
                "Area",
                "Area—Previous Study",
                "Year",
                "Road Description",
                "Average Speed (mph)",
            ],
            value_vars=[
                "BENZ",
                "NAPTH",
                "BUTA",
                "FORM",
                "ACROL",
                "DPM",
                "POM",
                "NOX",
                "PM10",
                "CO2",
                "PM25",
                "SO2",
                "NO2",
                "VOC",
                "CO",
            ],
            var_name="Pollutant",
            value_name="Previous Study Emission Rates (grams/mile)",
        )
    )
    erlt_dict_2014[new_district] = erlt_df_2014_tmp_2

erlt_df_2014 = pd.concat(erlt_dict_2014.values())

erlt_df_2014_2014b = pd.merge(
    left=erlt_df_2014b,
    right=erlt_df_2014,
    on=["Area", "Year", "Road Description", "Average Speed (mph)", "Pollutant"],
).assign(
    yearid_avgspeed=lambda df: df["Year"].astype(str)
    + "—"
    + df["Average Speed (mph)"].astype(str),
    per_diff=lambda df: np.round(
        (
            (
                df["2021 Emission Rates (grams/mile)"]
                - df["Previous Study Emission Rates (grams/mile)"]
            )
            / df["Previous Study Emission Rates (grams/mile)"]
        )
        * 100,
        2,
    ),
)

erlt_df_2014_2014b_1 = erlt_df_2014_2014b.rename(
    columns={
        "2021 Emission Rates (grams/mile)": "Current Study",
        "Previous Study Emission Rates (grams/mile)": "Previous Study",
        "per_diff": "Percent Change in Current Study Emissions",
    }
)

per_diff_df = erlt_df_2014_2014b_1[
    ['Area', 'Year', 'Road Description', 'Average Speed (mph)', 'Pollutant',
     'Percent Change in Current Study Emissions']]


erlt_df_2014_2014b_2 = erlt_df_2014_2014b_1.melt(
    id_vars=["Area", "Year", "Road Description", "Average Speed (mph)", "Pollutant"],
    value_vars=["Current Study", "Previous Study"],
    var_name="Study",
    value_name="Emission Rate (grams/mile)",
)

erlt_df_2014_2014b_3 = (
    pd.merge(erlt_df_2014_2014b_2,
             per_diff_df)
)

erlt_df_2014_2014b_3.to_csv(path_plotting_df)
erlt_df_2014_2014b_3.to_csv(path_plotting_df2)
