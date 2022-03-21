import pandas as pd
import os
import glob
from ttierlt_v1.utils import PATH_PROCESSED, PATH_TO_MARIA_DB_DATA, connect_to_server_db

pd.set_option("precision", 16)
conn = connect_to_server_db(database_nm="mvs2014b_erlt_out")
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
erlt_df_2014b_py = pd.read_sql(
    f"""SELECT * FROM starts_erlt_intermediate 
    WHERE Area IN {DISTRICTS_PRCSD_SQL_SAFE}; """,
    conn,
)
conn.close()

erlt_df_2014b_py_1 = (
    erlt_df_2014b_py.rename(
        columns={"VehicleType": "vehicletype", "FUELTYPE": "fueltype"}
    )
    .set_index(["Area", "yearid", "monthid", "vehicletype", "fueltype"])
    .stack()
    .reset_index()
    .rename(columns={"level_5": "pollutant", 0: "emis_rt"})
    .sort_values(["pollutant", "Area", "monthid", "vehicletype", "fueltype", "yearid"])
)


erlt_df_2014b_py_2 = erlt_df_2014b_py_1.assign(
    emis_rt_shift_down=lambda df: (
        df.groupby(
            ["pollutant", "vehicletype", "fueltype", "Area", "monthid"]
        ).emis_rt.shift(1)
    ),
    diff_emis_rt=lambda df: df.emis_rt - df.emis_rt_shift_down,
)

erlt_df_2014b_py_annomalies = erlt_df_2014b_py_2.loc[
    lambda df: df.diff_emis_rt > 0.0001
]


erlt_df_2014b_py_annual = (
    erlt_df_2014b_py_1.groupby(
        ["Area", "yearid", "vehicletype", "fueltype", "pollutant"]
    )
    .emis_rt.max()
    .reset_index()
    .sort_values(["pollutant", "Area", "yearid", "vehicletype", "fueltype"])
    .assign(
        emis_rt_shift_down=lambda df: (
            df.groupby(["pollutant", "Area", "vehicletype", "fueltype"]).emis_rt.shift(
                1
            )
        ),
        diff_emis_rt=lambda df: df.emis_rt - df.emis_rt_shift_down,
    )
)

erlt_df_2014b_py_annual_annomalies = erlt_df_2014b_py_annual.loc[
    lambda df: df.diff_emis_rt > 0
]


def debug_raw_startrate_tb(
    db_yr1,
    db_yr2,
    suffixes,
):
    district_yr1_db_conn = connect_to_server_db(db_yr1)
    startrt_district_yr1 = pd.read_sql("SELECT * FROM startrate", district_yr1_db_conn)
    district_yr1_db_conn.close()
    startrt_district_yr1_fil = startrt_district_yr1
    district_yr2_db_conn = connect_to_server_db(db_yr2)
    startrt_district_yr2 = pd.read_sql("SELECT * FROM startrate", district_yr2_db_conn)
    district_yr2_db_conn.close()
    startrt_district_yr2_fil = startrt_district_yr2

    yr1_suf = suffixes[0]
    yr2_suf = suffixes[1]
    startrt_district_yr1_yr2 = (
        pd.merge(
            startrt_district_yr1_fil,
            startrt_district_yr2_fil,
            on=[
                "Area",
                "monthid",
                "hourid",
                "sourcetypeid",
                "fueltypeid",
                "pollutantid",
                "VehicleType",
                "FuelType",
            ],
            suffixes=suffixes,
        )
        .assign(
            ERate_diff=lambda df: df[f"ERate{yr2_suf}"] - df[f"ERate{yr1_suf}"],
            emisFact_diff=lambda df: df[f"emisFact{yr2_suf}"]
            - df[f"emisFact{yr1_suf}"],
            is_discrepancy=lambda df: (df.emisFact_diff > 0.0000001).astype(int),
            is_erate_discrepency=lambda df: (df.ERate_diff > 0.0000001).astype(int),
            tot_emisfact_yr1=lambda df: df.groupby(
                ["Area", "monthid", "VehicleType", "FuelType", "pollutantid"]
            )[f"emisFact{yr1_suf}"].transform(sum),
            tot_emisfact_yr2=lambda df: df.groupby(
                ["Area", "monthid", "VehicleType", "FuelType", "pollutantid"]
            )[f"emisFact{yr2_suf}"].transform(sum),
            tot_emisfact_diff=lambda df: df.tot_emisfact_yr2 - df.tot_emisfact_yr1,
            is_discrepency_tot_diff=lambda df: (
                df.tot_emisfact_diff > 0.0000001
            ).astype(int),
        )
        .sort_values(
            [
                "is_discrepency_tot_diff",
                "pollutantid",
                "Area",
                "sourcetypeid",
                "fueltypeid",
            ],
            ascending=[False, True, True, True, True],
        )
        .reset_index(drop=True)
    )
    return startrt_district_yr1_yr2


elp_issue = debug_raw_startrate_tb(
    db_yr1="mvs14b_erlt_elp_48141_2026_7_cer_out",
    db_yr2="mvs14b_erlt_elp_48141_2028_7_cer_out",
    suffixes=["_26", "_28"],
)


path_out_running_debug = os.path.join(
    PATH_PROCESSED, "debug_inconsistent_patterns", "start_refuse_2026_28_debug.xlsx"
)
with pd.ExcelWriter(path_out_running_debug) as xlwr:
    elp_issue.to_excel(xlwr, sheet_name="El Paso")
