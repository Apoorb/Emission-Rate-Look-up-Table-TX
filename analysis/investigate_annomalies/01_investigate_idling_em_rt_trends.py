import pandas as pd
import os
import glob
import numpy as np
from ttierlt.utils import (
    PATH_PROCESSED, PATH_TO_MARIA_DB_DATA, connect_to_server_db
)


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
    f"""SELECT * FROM idling_erlt_intermediate 
    WHERE Area IN {DISTRICTS_PRCSD_SQL_SAFE}; """,
    conn,
)
conn.close()


erlt_df_2014b_py_1 = (
    erlt_df_2014b_py
    .set_index(['Area', 'yearid', 'monthid', 'hourid', 'period'])
    .stack()
    .reset_index()
    .rename(columns={"level_5": "pollutant", 0: "emis_rt"})
    .sort_values(['pollutant', 'Area', 'monthid', 'hourid', 'period', 'yearid'])
)

erlt_df_2014b_py_2 = (
    erlt_df_2014b_py_1
    .assign(
        emis_rt_shift_down=lambda df: (
            df.groupby(['pollutant', 'Area', 'monthid', 'hourid', 'period'])
            .emis_rt.shift(1)
        ),
        diff_emis_rt=lambda df: df.emis_rt - df.emis_rt_shift_down,
    )
)

erlt_df_2014b_py_annomalies = (
    erlt_df_2014b_py_2
    .loc[lambda df: df.diff_emis_rt > 0]
)
annomaly_area_pol = erlt_df_2014b_py_annomalies.groupby(
    ['pollutant', 'Area']).groups.keys()


erlt_df_2014b_py_annual = (
    erlt_df_2014b_py_1
    .groupby(['Area', 'yearid', 'pollutant'])
    .emis_rt.max()
    .reset_index()
    .sort_values(['pollutant', 'Area', 'yearid'])
    .assign(
        emis_rt_shift_down=lambda df: (
            df.groupby(['pollutant', 'Area'])
                .emis_rt.shift(1)
        ),
        diff_emis_rt=lambda df: df.emis_rt - df.emis_rt_shift_down,
        percent_diff=lambda df: np.round(100 * df.diff_emis_rt / df.emis_rt, 3)
    )
)

erlt_df_2014b_py_annual_annomalies = (
    erlt_df_2014b_py_annual
    .loc[lambda df: df.diff_emis_rt > 0]
)

erlt_df_2014b_py_annual_annomalies.percent_diff.max()

pattern_district_elp = os.path.join(
    PATH_TO_MARIA_DB_DATA, f"mvs14b_erlt_elp_*_203[68]_per_out"
)
db_dirs_county_year_month = glob.glob(pattern_district_elp)
db_nms_county_year_month = [
    os.path.basename(dir_path) for dir_path in db_dirs_county_year_month
]


def debug_raw_idlerate_tb(db_yr1, db_yr2, suffixes, pol_map = {33: "NO2",
                                                               3: "NOX",
                                                               31: "SO2",
                                                               98: "CO2EQ"}):
    district_yr1_db_conn = connect_to_server_db(db_yr1)
    idlert_district_yr1 = pd.read_sql("SELECT * FROM idlerate", district_yr1_db_conn)
    district_yr1_db_conn.close()
    idlert_district_yr1_fil = (
        idlert_district_yr1
        .assign(pollutant=lambda df: df.pollutantid.map(pol_map))
        .loc[lambda df:  (~df.idlerate.isna()) & (~ df.pollutant.isna())]
    )
    district_yr2_db_conn = connect_to_server_db(db_yr2)
    idlert_district_yr2 = pd.read_sql("SELECT * FROM idlerate", district_yr2_db_conn)
    district_yr2_db_conn.close()
    idlert_district_yr2_fil = (
        idlert_district_yr2
        .assign(pollutant=lambda df: df.pollutantid.map(pol_map))
        .loc[lambda df:  (~df.idlerate.isna()) & (~ df.pollutant.isna())]
    )

    yr1_suf = suffixes[0]
    yr2_suf = suffixes[1]
    idlert_district_yr1_yr2 = (
        pd.merge(
            idlert_district_yr1_fil,
            idlert_district_yr2_fil,
            on=['Area', 'monthid', 'period', 'hourid', 'countyid', 'linkid',
                'pollutantid', 'pollutant', 'sourcetypeid', 'fueltypeid'],
            suffixes=suffixes
        )
        .assign(
            emission_diff=lambda df: df[f"emission{yr2_suf}"]
                                     - df[f"emission{yr1_suf}"],
            emisfact_diff=lambda df: df[f"emisfact{yr2_suf}"]
                                     - df[f"emisfact{yr1_suf}"],
            is_discrepancy=lambda df: (df.emisfact_diff > 0.00001).astype(int),
            is_discrepancy_emission=lambda df: (df.emission_diff > 0.00001).astype(int),
            tot_emisfact_yr1=lambda df: df.groupby([
                'Area', 'monthid', 'period', 'hourid', 'countyid',
                'pollutant'])[f"emisfact{yr1_suf}"].transform(sum),
            tot_emisfact_yr2=lambda df: df.groupby([
                'Area', 'monthid', 'period', 'hourid', 'countyid',
                'pollutant'])[f"emisfact{yr2_suf}"].transform(sum),
            tot_emisfact_diff=lambda df: df.tot_emisfact_yr2
                                         - df.tot_emisfact_yr1,
            is_discrepency_tot_diff=lambda df: (df.tot_emisfact_diff >
                                                0.00001).astype(int)
        )
        .sort_values(["is_discrepency_tot_diff", "pollutant", 'Area',
                          'monthid',
                          'period', 'hourid'],
                         ascending=[False, True, True, True, True, True])
        .reset_index(drop=True)
    )
    return idlert_district_yr1_yr2


elp_issue = debug_raw_idlerate_tb(
    db_yr1="mvs14b_erlt_elp_48141_2036_per_out",
    db_yr2="mvs14b_erlt_elp_48141_2038_per_out",
    suffixes=["_36", "_38"]
)
ftw_issue = debug_raw_idlerate_tb(
    db_yr1="mvs14b_erlt_ftw_48439_2036_per_out",
    db_yr2="mvs14b_erlt_ftw_48439_2038_per_out",
    suffixes = ["_36", "_38"]
)
aus_issue = debug_raw_idlerate_tb(
    db_yr1="mvs14b_erlt_aus_48453_2036_per_out",
    db_yr2="mvs14b_erlt_aus_48453_2038_per_out",
    suffixes = ["_36", "_38"]
)
sat_issue = debug_raw_idlerate_tb(
    db_yr1="mvs14b_erlt_sat_48029_2036_per_out",
    db_yr2="mvs14b_erlt_sat_48029_2038_per_out",
    suffixes = ["_36", "_38"]
)
elp_no_issue = debug_raw_idlerate_tb(
    db_yr1="mvs14b_erlt_elp_48141_2026_per_out",
    db_yr2="mvs14b_erlt_elp_48141_2028_per_out",
    suffixes=["_26", "_28"]
)

path_out_idle_debug = os.path.join(PATH_PROCESSED,
                                   "debug_inconsistent_patterns",
                                   "idle_NO2_2038_rt_inc_debug.xlsx")
with pd.ExcelWriter(path_out_idle_debug) as xlwr:
    aus_issue.to_excel(xlwr, sheet_name="Austin")
    elp_issue.to_excel(xlwr, sheet_name="El Paso")
    elp_no_issue.to_excel(xlwr, sheet_name="El Paso---No Issue")
    ftw_issue.to_excel(xlwr, sheet_name="Fort Worth")
    sat_issue.to_excel(xlwr, sheet_name="San Antonio")



