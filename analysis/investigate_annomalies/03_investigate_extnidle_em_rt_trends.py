import pandas as pd
import os
import glob
from ttierlt_v1.utils import PATH_PROCESSED, PATH_TO_MARIA_DB_DATA, connect_to_server_db


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
    f"""SELECT * FROM extnidle_erlt_intermediate WHERE Area 
    IN {DISTRICTS_PRCSD_SQL_SAFE}; """,
    conn,
)
conn.close()


erlt_df_2014b_py_1 = (
    erlt_df_2014b_py.set_index(["Area", "yearid", "monthid", "Processtype"])
    .stack()
    .reset_index()
    .rename(columns={"level_4": "pollutant", 0: "emis_rt"})
    .sort_values(["pollutant", "Area", "monthid", "Processtype", "yearid"])
)

erlt_df_2014b_py_2 = erlt_df_2014b_py_1.assign(
    emis_rt_shift_down=lambda df: (
        df.groupby(["pollutant", "Area", "monthid", "Processtype"]).emis_rt.shift(1)
    ),
    diff_emis_rt=lambda df: df.emis_rt - df.emis_rt_shift_down,
)

erlt_df_2014b_py_annomalies = erlt_df_2014b_py_2.loc[lambda df: df.diff_emis_rt > 0]
annomaly_area_pol = erlt_df_2014b_py_annomalies.groupby(
    ["pollutant", "Area"]
).groups.keys()


erlt_df_2014b_py_annual = (
    erlt_df_2014b_py_1.groupby(["Area", "yearid", "Processtype", "pollutant"])
    .emis_rt.max()
    .reset_index()
    .sort_values(["pollutant", "Area", "Processtype", "yearid"])
    .assign(
        emis_rt_shift_down=lambda df: (
            df.groupby(["pollutant", "Area"]).emis_rt.shift(1)
        ),
        diff_emis_rt=lambda df: df.emis_rt - df.emis_rt_shift_down,
    )
)

erlt_df_2014b_py_annual_annomalies = erlt_df_2014b_py_annual.loc[
    lambda df: df.diff_emis_rt > 0
]


def debug_raw_extndidlerate_tb(db_yr1, db_yr2, suffixes):
    district_yr1_db_conn = connect_to_server_db(db_yr1)
    extndidle_district_yr1 = pd.read_sql(
        "SELECT * FROM extnidlerate", district_yr1_db_conn
    )
    district_yr1_db_conn.close()
    extndidle_district_yr1_fil = extndidle_district_yr1.loc[
        lambda df: (~df.pollutantID.isna())
    ]
    district_yr2_db_conn = connect_to_server_db(db_yr2)
    extndidle_district_yr2 = pd.read_sql(
        "SELECT * FROM extnidlerate", district_yr2_db_conn
    )
    district_yr2_db_conn.close()
    extndidle_district_yr2_fil = extndidle_district_yr2.loc[
        lambda df: (~df.pollutantID.isna())
    ]

    yr1_suf = suffixes[0]
    yr2_suf = suffixes[1]
    extndidle_district_yr1_yr2 = (
        pd.merge(
            extndidle_district_yr1_fil,
            extndidle_district_yr2_fil,
            on=[
                "Area",
                "monthid",
                "hourID",
                "pollutantID",
                "processid",
                "Processtype",
                "sourceTypeID",
                "sourceTypeID",
                "fuelTypeID",
            ],
            suffixes=suffixes,
        )
        .assign(
            rateperhour_diff=lambda df: df[f"rateperhour{yr2_suf}"]
            - df[f"rateperhour{yr1_suf}"],
            emisFact_diff=lambda df: df[f"emisFact{yr2_suf}"]
            - df[f"emisFact{yr1_suf}"],
            is_discrepancy=lambda df: (df.emisFact_diff > 0.00001).astype(int),
            is_discrepancy_emission=lambda df: (df.rateperhour_diff > 0.00001).astype(
                int
            ),
            tot_emisfact_yr1=lambda df: df.groupby(
                ["Area", "monthid", "pollutantID", "Processtype"]
            )[f"emisFact{yr1_suf}"].transform(sum),
            tot_emisfact_yr2=lambda df: df.groupby(
                ["Area", "monthid", "pollutantID", "Processtype"]
            )[f"emisFact{yr2_suf}"].transform(sum),
            tot_emisfact_diff=lambda df: df.tot_emisfact_yr2 - df.tot_emisfact_yr1,
            is_discrepency_tot_diff=lambda df: (df.tot_emisfact_diff > 0.00001).astype(
                int
            ),
        )
        .sort_values(
            ["pollutantID", "Area", "monthid", "hourID", "Processtype"],
            ascending=[True, True, True, True, True],
        )
        .reset_index(drop=True)
    )
    return extndidle_district_yr1_yr2


wac_issue = debug_raw_extndidlerate_tb(
    db_yr1="mvs14b_erlt_wac_48309_2030_7_cer_out",
    db_yr2="mvs14b_erlt_wac_48309_2050_7_cer_out",
    suffixes=["_30", "_50"],
)


path_out_running_debug = os.path.join(
    PATH_PROCESSED, "debug_inconsistent_patterns", "extnidle_const_rate_debug.xlsx"
)
with pd.ExcelWriter(path_out_running_debug) as xlwr:
    wac_issue.to_excel(xlwr, sheet_name="Waco")
