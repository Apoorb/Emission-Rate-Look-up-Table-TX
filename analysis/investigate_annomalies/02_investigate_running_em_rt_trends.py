import pandas as pd
import os
import glob
import numpy as np
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
    f"""SELECT * FROM running_erlt_intermediate 
    WHERE Area IN {DISTRICTS_PRCSD_SQL_SAFE}; """,
    conn,
)
conn.close()

erlt_df_2014b_py_1 = (
    erlt_df_2014b_py.set_index(["Area", "yearid", "monthid", "funclass", "avgspeed"])
    .stack()
    .reset_index()
    .rename(columns={"level_5": "pollutant", 0: "emis_rt"})
    .sort_values(["pollutant", "Area", "monthid", "funclass", "avgspeed", "yearid"])
)


erlt_df_2014b_py_2 = erlt_df_2014b_py_1.assign(
    emis_rt_shift_down=lambda df: (
        df.groupby(
            ["pollutant", "Area", "monthid", "funclass", "avgspeed"]
        ).emis_rt.shift(1)
    ),
    diff_emis_rt=lambda df: df.emis_rt - df.emis_rt_shift_down,
)

erlt_df_2014b_py_2_dal_so2_44_46 = erlt_df_2014b_py_2.loc[
    lambda df: (df.Area == "Dallas")
    & (df.pollutant == "SO2")
    & (df.yearid.isin([2044, 2046]))
]

erlt_df_2014b_py_annomalies = erlt_df_2014b_py_2.loc[lambda df: df.diff_emis_rt > 0]

erlt_df_2014b_py_annomalies_dal = erlt_df_2014b_py_annomalies.loc[
    lambda df: df.Area == "Dallas"
]

erlt_df_2014b_py_annomalies_ftw = erlt_df_2014b_py_annomalies.loc[
    lambda df: df.Area == "Fort Worth"
]


erlt_df_2014b_py_annomalies[lambda df: df.pollutant == "NAPTH"].eval(
    "emis_rt - emis_rt_shift_down"
) / 0.00004
problem_area_years_pol = erlt_df_2014b_py_annomalies.groupby(["Area"]).agg(
    yearid=("yearid", set), pollutant=("pollutant", set)
)

erlt_df_2014b_py_annual = (
    erlt_df_2014b_py_1.groupby(["Area", "yearid", "funclass", "avgspeed", "pollutant"])
    .emis_rt.max()
    .reset_index()
    .sort_values(["pollutant", "Area", "yearid", "funclass", "avgspeed"])
    .assign(
        emis_rt_shift_down=lambda df: (
            df.groupby(["pollutant", "Area", "funclass", "avgspeed"]).emis_rt.shift(1)
        ),
        diff_emis_rt=lambda df: df.emis_rt - df.emis_rt_shift_down,
        percent_diff=lambda df: np.round(100 * df.diff_emis_rt / df.emis_rt, 3),
    )
)


test = erlt_df_2014b_py_annual.loc[
    lambda df: (df.Area == "Dallas") & (df.pollutant == "CO")
]
erlt_df_2014b_py_annual_annomalies = erlt_df_2014b_py_annual.loc[
    lambda df: df.diff_emis_rt > 0
]
erlt_df_2014b_py_annual_annomalies.percent_diff.max()
erlt_df_2014b_py_annual_annomalies_sets = erlt_df_2014b_py_annual_annomalies.groupby(
    ["pollutant", "Area"]
).agg(
    funclass=("funclass", set),
    avgspeed=("avgspeed", set),
    yearid=("yearid", set),
    percent_diff=("percent_diff", list),
)


pattern_district_elp = os.path.join(
    PATH_TO_MARIA_DB_DATA, f"mvs14b_erlt_wac_*_204[24]_7_cer_out"
)
db_dirs_county_year_month = glob.glob(pattern_district_elp)
db_nms_county_year_month = [
    os.path.basename(dir_path) for dir_path in db_dirs_county_year_month
]


def debug_raw_runningrate_tb(
    db_yr1, db_yr2, suffixes, pol_map={31: "SO2", 98: "CO2EQ"}
):
    district_yr1_db_conn = connect_to_server_db(db_yr1)
    runrt_district_yr1 = pd.read_sql("SELECT * FROM emisrate", district_yr1_db_conn)
    district_yr1_db_conn.close()
    runrt_district_yr1_fil = runrt_district_yr1.assign(
        pollutant=lambda df: df.pollutantid.map(pol_map)
    ).loc[lambda df: (~df.pollutant.isna())]
    district_yr2_db_conn = connect_to_server_db(db_yr2)
    runrt_district_yr2 = pd.read_sql("SELECT * FROM emisrate", district_yr2_db_conn)
    district_yr2_db_conn.close()
    runrt_district_yr2_fil = runrt_district_yr2.assign(
        pollutant=lambda df: df.pollutantid.map(pol_map)
    ).loc[lambda df: (~df.pollutant.isna())]

    yr1_suf = suffixes[0]
    yr2_suf = suffixes[1]
    runrt_district_yr1_yr2 = (
        pd.merge(
            runrt_district_yr1_fil,
            runrt_district_yr2_fil,
            on=[
                "Area",
                "monthid",
                "hourid",
                "Period",
                "roadtypeid",
                "Funclass",
                "pollutantid",
                "pollutant",
                "sourcetypeid",
                "fueltypeid",
                "avgSpeedBinID",
                "avgspeed",
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
                ["Area", "monthid", "Funclass", "avgspeed", "pollutant"]
            )[f"emisFact{yr1_suf}"].transform(sum),
            tot_emisfact_yr2=lambda df: df.groupby(
                ["Area", "monthid", "Funclass", "avgspeed", "pollutant"]
            )[f"emisFact{yr2_suf}"].transform(sum),
            tot_emisfact_diff=lambda df: df.tot_emisfact_yr2 - df.tot_emisfact_yr1,
            is_discrepency_tot_diff=lambda df: (
                df.tot_emisfact_diff > 0.0000001
            ).astype(int),
            is_stypemix_increasing=lambda df: (
                (df[f"stypemix{yr2_suf}"] - df[f"stypemix{yr1_suf}"]) > 0.0000001
            ).astype(int),
        )
        .sort_values(
            [
                "is_discrepency_tot_diff",
                "pollutant",
                "Area",
                "Funclass",
                "avgSpeedBinID",
                "monthid",
                "hourid",
            ],
            ascending=[False, True, True, True, True, True, True],
        )
        .reset_index(drop=True)
    )
    return runrt_district_yr1_yr2


wac_issue = debug_raw_runningrate_tb(
    db_yr1="mvs14b_erlt_wac_48309_2042_7_cer_out",
    db_yr2="mvs14b_erlt_wac_48309_2044_7_cer_out",
    suffixes=["_42", "_44"],
)


ftw_issue = debug_raw_runningrate_tb(
    db_yr1="mvs14b_erlt_ftw_48439_2020_7_cer_out",
    db_yr2="mvs14b_erlt_ftw_48439_2022_7_cer_out",
    suffixes=["_20", "_22"],
)

dal_issue = debug_raw_runningrate_tb(
    db_yr1="mvs14b_erlt_dal_48113_2038_7_cer_out",
    db_yr2="mvs14b_erlt_dal_48113_2040_7_cer_out",
    suffixes=["_38", "_40"],
    pol_map={23: "NAPTH", 185: "NAPTH"},
)

dal_issue2 = debug_raw_runningrate_tb(
    db_yr1="mvs14b_erlt_dal_48113_2044_1_cer_out",
    db_yr2="mvs14b_erlt_dal_48113_2046_1_cer_out",
    suffixes=["_44", "_46"],
    pol_map={2: "CO", 3: "NOX", 31: "SO2"},
)

dal_issue2_so2 = (
    dal_issue2.loc[lambda df: (df.pollutant == "SO2") & (df.avgspeed == 2.5)]
    .groupby(["roadtypeid", "pollutant"])
    .agg(
        emisFact_44=("emisFact_44", "sum"),
        emisFact_46=("emisFact_46", "sum"),
        tot_emisfact_diff=("tot_emisfact_diff", "mean"),
    )
)

elp_no_issue = debug_raw_runningrate_tb(
    db_yr1="mvs14b_erlt_elp_48141_2042_7_cer_out",
    db_yr2="mvs14b_erlt_elp_48141_2044_7_cer_out",
    suffixes=["_42", "_44"],
)


path_out_running_debug = os.path.join(
    PATH_PROCESSED,
    "debug_inconsistent_patterns",
    "running_co2_so2_napth_rt_inc_debug.xlsx",
)
with pd.ExcelWriter(path_out_running_debug) as xlwr:
    elp_no_issue.to_excel(xlwr, sheet_name="El Paso---No Issue")
    ftw_issue.to_excel(xlwr, sheet_name="Fort Worth")
    wac_issue.to_excel(xlwr, sheet_name="Waco")
    dal_issue.to_excel(xlwr, sheet_name="Dallas Napth Issue")
    dal_issue2.to_excel(xlwr, sheet_name="Dallas co_nox_so2 Issue")
