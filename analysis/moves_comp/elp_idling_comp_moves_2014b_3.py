"""
Compare El Paso 4 year of idling emission rates between MOVES 2014b and 3 by
    hour (9),
    source use type (Combination long haul and passenger car),
    for NOx and VOC
"""

import pandas as pd
import numpy as np
import os
import inflection
from ttierlt.utils import connect_to_server_db, PATH_PROCESSED

if __name__ == "__main__":
    out_dir = os.path.join(PATH_PROCESSED, "moves_2014b_3")
    out_dir2 = r"C:\Users\a-bibeka\PycharmProjects\idle_moves_2014b_vs_3\data"
    if not os.path.exists(out_dir):
        os.mkdir(out_dir)
    out_path = os.path.join(out_dir, "idling_mvs_2014b_3.csv")
    out_path2 = os.path.join(out_dir2, "idling_mvs_2014b_3.csv")

    conn = connect_to_server_db(database_nm="movesdb20210209")
    emissionprocess = pd.read_sql("SELECT * FROM emissionprocess", conn)
    fueltype = pd.read_sql("SELECT * FROM fueltype", conn)
    pollutant = pd.read_sql("SELECT * FROM pollutant", conn)
    sourceusetype = pd.read_sql("SELECT * FROM sourceusetype", conn)

    emissionprocess1 = (
        emissionprocess.rename(
            columns={col: inflection.underscore(col) for col in emissionprocess.columns}
        ).filter(items=["process_id", "process_name", "short_name"])
    ).rename(columns={"short_name": "process_short_name"})

    fueltype1 = fueltype.rename(
        columns={col: inflection.underscore(col) for col in fueltype.columns}
    ).filter(items=["fuel_type_id", "fuel_type_desc"])

    pollutant1 = (
        pollutant.rename(
            columns={col: inflection.underscore(col) for col in pollutant.columns}
        ).filter(items=["pollutant_id", "pollutant_name", "short_name"])
    ).rename(columns={"short_name": "pollutant_short_name"})

    sourceusetype1 = sourceusetype.rename(
        columns={col: inflection.underscore(col) for col in sourceusetype.columns}
    ).filter(items=["source_type_id", "source_type_name"])

    mvs2014b_df = {
        "elp_2020_7": {
            "db": "mvs14b_erlt_elp_48141_2020_per_out",
            "df": pd.DataFrame(),
        },
        "elp_2030_7": {
            "db": "mvs14b_erlt_elp_48141_2030_per_out",
            "df": pd.DataFrame(),
        },
        "elp_2040_7": {
            "db": "mvs14b_erlt_elp_48141_2040_per_out",
            "df": pd.DataFrame(),
        },
        "elp_2050_7": {
            "db": "mvs14b_erlt_elp_48141_2050_per_out",
            "df": pd.DataFrame(),
        },
    }
    mvs3_df = {
        "elp_2020_7": {
            "db": "mvs3_erlt_elp_48141_2020_7_cer_out",
            "df": pd.DataFrame(),
        },
        "elp_2030_7": {
            "db": "mvs3_erlt_elp_48141_2030_7_cer_out",
            "df": pd.DataFrame(),
        },
        "elp_2040_7": {
            "db": "mvs3_erlt_elp_48141_2040_7_cer_out",
            "df": pd.DataFrame(),
        },
        "elp_2050_7": {
            "db": "mvs3_erlt_elp_48141_2050_7_cer_out",
            "df": pd.DataFrame(),
        },
    }

    def process_mvs2014b_df(mvs2014b_df_, key_):
        conn = connect_to_server_db(database_nm=mvs2014b_df_[key_]["db"])
        idling_mvs2014b = pd.read_sql(
            f"""SELECT * FROM movesoutput 
            WHERE hourid = 8 AND pollutantID IN (2, 3, 87)
            AND sourceTypeID in (21, 62)
            AND monthID = 7
            AND emissionQuant <>0
            """,
            conn,
        )
        movesactivityoutput_2014b = pd.read_sql(
            """
            SELECT yearID, monthID, hourID, linkID, sourceTypeID, fuelTypeID,
            SUM(activity)as vmx FROM movesactivityoutput
            WHERE activitytypeid = 4 and activity > 0
            GROUP BY yearid,monthid,hourid,countyid,linkid,sourcetypeid,fueltypeid;
            """,
            conn,
        )
        conn.close()

        mvs_lab = "MOVES 2014b"
        idling_mvs2014b_1 = (
            idling_mvs2014b.merge(
                movesactivityoutput_2014b,
                on=[
                    "yearID",
                    "monthID",
                    "hourID",
                    "linkID",
                    "sourceTypeID",
                    "fuelTypeID",
                ],
                how="left",
            )
            .rename(
                columns={
                    col: inflection.underscore(col) for col in idling_mvs2014b.columns
                }
            )
            .assign(
                rate_per_hour=lambda df: df.emission_quant / df.vmx,
                moves=mvs_lab,
                day=lambda df: df.day_id.map({5: "Weekday", 2: "Weekend"}),
                month=lambda df: df.month_id.map({7: "July"}),
            )
            .merge(emissionprocess1, on="process_id", how="left")
            .merge(sourceusetype1, on="source_type_id", how="left")
            .merge(fueltype1, on="fuel_type_id", how="left")
            .merge(pollutant1, on="pollutant_id", how="left")
            .filter(
                items=[
                    "moves",
                    "year_id",
                    "month",
                    "day",
                    "hour_id",
                    "source_type_id",
                    "fuel_type_id",
                    "pollutant_id",
                    "process_id",
                    "process_name",
                    "process_short_name",
                    "source_type_name",
                    "fuel_type_desc",
                    "pollutant_name",
                    "pollutant_short_name",
                    "rate_per_hour",
                ]
            )
            .sort_values(
                by=[
                    "moves",
                    "year_id",
                    "month",
                    "day",
                    "hour_id",
                    "source_type_id",
                    "fuel_type_id",
                    "pollutant_id",
                ]
            )
            .reset_index(drop=True)
        )

        mean_count = (
            idling_mvs2014b_1.groupby(
                [
                    "moves",
                    "year_id",
                    "month",
                    "day",
                    "hour_id",
                    "source_type_id",
                    "fuel_type_id",
                    "pollutant_id",
                    "process_id",
                ]
            )
            .rate_per_hour.count()
            .mean()
        )

        var_count = (
            idling_mvs2014b_1.groupby(
                [
                    "moves",
                    "year_id",
                    "month",
                    "day",
                    "hour_id",
                    "source_type_id",
                    "fuel_type_id",
                    "pollutant_id",
                    "process_id",
                ]
            )
            .rate_per_hour.count()
            .var()
        )

        assert (mean_count == 1) & (var_count == 0), (
            "Look for multiple MOVES run in the output. Either filter them "
            "or delete them."
        )
        assert ~idling_mvs2014b_1.isna().any().any(), "Check for missing " "values."
        assert ~idling_mvs2014b_1.isna().any().any(), "Check for missing " "values."
        return idling_mvs2014b_1

    def process_mvs3_df(mvs3_df_, key_):
        conn = connect_to_server_db(database_nm=mvs3_df_[key_]["db"])
        idling_mvs3 = pd.read_sql(
            f"""SELECT * FROM rateperdistance 
            WHERE hourid = 9 AND pollutantID IN (2, 3, 87, 100, 110)
            AND sourceTypeID in (21, 62)
            AND avgSpeedBinID = 0
            """,
            conn,
        )
        conn.close()

        mvs_lab = "MOVES 3"
        idling_mvs3_1 = (
            idling_mvs3.rename(
                columns={col: inflection.underscore(col) for col in idling_mvs3.columns}
            )
            .assign(
                moves=mvs_lab,
                day=lambda df: df.day_id.map({5: "Weekday", 2: "Weekend"}),
                month=lambda df: df.month_id.map({7: "July"}),
            )
            .merge(emissionprocess1, on="process_id", how="left")
            .merge(sourceusetype1, on="source_type_id", how="left")
            .merge(fueltype1, on="fuel_type_id", how="left")
            .merge(pollutant1, on="pollutant_id", how="left")
            .filter(
                items=[
                    "moves",
                    "year_id",
                    "month",
                    "day",
                    "hour_id",
                    "source_type_id",
                    "fuel_type_id",
                    "pollutant_id",
                    "process_id",
                    "process_name",
                    "process_short_name",
                    "source_type_name",
                    "fuel_type_desc",
                    "pollutant_name",
                    "pollutant_short_name",
                    "rate_per_distance",
                ]
            )
            .sort_values(
                by=[
                    "moves",
                    "year_id",
                    "month",
                    "day",
                    "hour_id",
                    "source_type_id",
                    "fuel_type_id",
                    "pollutant_id",
                ]
            )
            .reset_index(drop=True)
        ).rename(columns={"rate_per_distance": "rate_per_hour"})

        mean_count = (
            idling_mvs3_1.groupby(
                [
                    "moves",
                    "year_id",
                    "month",
                    "day",
                    "hour_id",
                    "source_type_id",
                    "fuel_type_id",
                    "pollutant_id",
                    "process_id",
                ]
            )
            .rate_per_hour.count()
            .mean()
        )

        var_count = (
            idling_mvs3_1.groupby(
                [
                    "moves",
                    "year_id",
                    "month",
                    "day",
                    "hour_id",
                    "source_type_id",
                    "fuel_type_id",
                    "pollutant_id",
                    "process_id",
                ]
            )
            .rate_per_hour.count()
            .var()
        )

        assert (mean_count == 1) & (var_count == 0), (
            "Look for multiple MOVES run in the output. Either filter them "
            "or delete them."
        )
        assert ~idling_mvs3_1.isna().any().any(), "Check for missing " "values."
        assert ~idling_mvs3_1.isna().any().any(), "Check for missing " "values."
        return idling_mvs3_1

    for key in mvs3_df.keys():
        mvs2014b_df[key]["df"] = process_mvs2014b_df(mvs2014b_df_=mvs2014b_df, key_=key)

        mvs3_df[key]["df"] = process_mvs3_df(mvs3_df_=mvs3_df, key_=key)

    final_df = pd.concat(
        [
            value2
            for value in mvs2014b_df.values()
            for key2, value2 in value.items()
            if key2 == "df"
        ]
        + [
            value2
            for value in mvs3_df.values()
            for key2, value2 in value.items()
            if key2 == "df"
        ]
    )

    final_df_agg = (
        final_df.groupby(
            [
                "moves",
                "year_id",
                "month",
                "day",
                "source_type_id",
                "source_type_name",
                "fuel_type_id",
                "fuel_type_desc",
                "pollutant_id",
                "pollutant_short_name",
            ]
        )
        .agg(rate_per_hour=("rate_per_hour", "sum"))
        .reset_index()
    )

    temp_per_diff = final_df_agg.set_index(
        [
            "year_id",
            "month",
            "day",
            "source_type_id",
            "source_type_name",
            "fuel_type_id",
            "fuel_type_desc",
            "pollutant_id",
            "pollutant_short_name",
            "moves",
        ]
    ).unstack()

    temp_per_diff["per_diff"] = np.round(
        (
            (
                temp_per_diff[("rate_per_hour", "MOVES 3")]
                - temp_per_diff[("rate_per_hour", "MOVES 2014b")]
            )
            / temp_per_diff[("rate_per_hour", "MOVES 2014b")]
        )
        * 100,
        2,
    )

    temp_per_diff = (
        temp_per_diff.reset_index().drop("rate_per_hour", axis=1).droplevel(1, axis=1)
    )
    final_df_agg_1 = final_df_agg.merge(
        temp_per_diff,
        on=[
            "year_id",
            "month",
            "day",
            "source_type_id",
            "source_type_name",
            "fuel_type_id",
            "fuel_type_desc",
            "pollutant_id",
            "pollutant_short_name",
        ],
    )

    final_df_agg_1.to_csv(out_path)
    final_df_agg_1.to_csv(out_path2)
