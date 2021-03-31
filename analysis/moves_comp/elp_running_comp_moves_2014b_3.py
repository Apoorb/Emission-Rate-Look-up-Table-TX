"""
Compare El Paso 4 year of running emission rates between MOVES 2014b and 3 by
    hour (9),
    source use type (Combination long haul and passenger car),
    road type
    average speed
    for NOx and VOC
"""

import pandas as pd
import numpy as np
import os
import inflection
from ttierlt.utils import connect_to_server_db, PATH_PROCESSED


if __name__ == "__main__":
    out_dir = os.path.join(PATH_PROCESSED, "moves_2014b_3")
    out_dir2 = (
        r"C:\Users\a-bibeka\PycharmProjects\running_moves_2014b_vs_3_v1\data")
    if not os.path.exists(out_dir):
        os.mkdir(out_dir)
    out_path = os.path.join(out_dir, "running_mvs_2014b_3.csv")
    out_path2 = os.path.join(out_dir2, "running_mvs_2014b_3.csv")

    conn = connect_to_server_db(database_nm="movesdb20210209")
    avgspeedbin = pd.read_sql("SELECT * FROM avgspeedbin", conn)
    emissionprocess = pd.read_sql("SELECT * FROM emissionprocess", conn)
    fueltype = pd.read_sql("SELECT * FROM fueltype", conn)
    pollutant = pd.read_sql("SELECT * FROM pollutant", conn)
    roadtype = pd.read_sql("SELECT * FROM roadtype", conn)
    sourceusetype = pd.read_sql("SELECT * FROM sourceusetype", conn)

    avgspeedbin1 = (
        avgspeedbin.rename(
            columns={col: inflection.underscore(col) for col in avgspeedbin.columns}
        )
        .assign(
            avg_bin_speed=lambda df: df.avg_bin_speed.astype(float).round(1),
        )
        .filter(items=["avg_speed_bin_id", "avg_bin_speed", "avg_speed_bin_desc"])
    )

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

    roadtype1 = roadtype.rename(
        columns={col: inflection.underscore(col) for col in roadtype.columns}
    ).filter(items=["road_type_id", "road_desc"])

    sourceusetype1 = sourceusetype.rename(
        columns={col: inflection.underscore(col) for col in sourceusetype.columns}
    ).filter(items=["source_type_id", "source_type_name"])

    mvs2014b_df = {
        "elp_2020_7": {
            "db": "mvs14b_erlt_elp_48141_2020_7_cer_out",
            "df": pd.DataFrame(),
        },
        "elp_2030_7": {
            "db": "mvs14b_erlt_elp_48141_2030_7_cer_out",
            "df": pd.DataFrame(),
        },
        "elp_2040_7": {
            "db": "mvs14b_erlt_elp_48141_2040_7_cer_out",
            "df": pd.DataFrame(),
        },
        "elp_2050_7": {
            "db": "mvs14b_erlt_elp_48141_2050_7_cer_out",
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
    for key in mvs3_df.keys():
        for mvs_lab, mvs_df in zip(["MOVES 2014b", "MOVES 3"], [mvs2014b_df, mvs3_df]):
            conn = connect_to_server_db(database_nm=mvs_df[key]["db"])
            running_mvs = pd.read_sql(
                f"""SELECT * FROM rateperdistance 
                WHERE hourid = 9 AND pollutantID IN (2, 3, 87, 100, 110)
                AND processid not in (18,19)
                AND sourceTypeID in (21, 62)
                AND avgSpeedBinID <> 0
                """,
                conn,
            )
            conn.close()

            running_mvs1 = (
                running_mvs.rename(
                    columns={
                        col: inflection.underscore(col) for col in running_mvs.columns
                    }
                )
                .assign(
                    moves=mvs_lab,
                    day=lambda df: df.day_id.map({5: "Weekday", 2: "Weekend"}),
                    month=lambda df: df.month_id.map({7: "July"}),
                )
                .merge(avgspeedbin1, on="avg_speed_bin_id", how="left")
                .merge(emissionprocess1, on="process_id", how="left")
                .merge(sourceusetype1, on="source_type_id", how="left")
                .merge(fueltype1, on="fuel_type_id", how="left")
                .merge(roadtype1, on="road_type_id", how="left")
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
                        "avg_speed_bin_id",
                        "road_type_id",
                        "process_id",
                        "avg_bin_speed",
                        "avg_speed_bin_desc",
                        "process_name",
                        "process_short_name",
                        "source_type_name",
                        "fuel_type_desc",
                        "road_desc",
                        "pollutant_name",
                        "pollutant_short_name",
                        "rate_per_distance",
                    ]
                )
            )

            mean_count = (
                running_mvs1.groupby(
                    [
                        "moves",
                        "year_id",
                        "month",
                        "day",
                        "hour_id",
                        "source_type_id",
                        "fuel_type_id",
                        "pollutant_id",
                        "avg_speed_bin_id",
                        "road_type_id",
                        "process_id",
                    ]
                )
                .rate_per_distance.count()
                .mean()
            )

            var_count = (
                running_mvs1.groupby(
                    [
                        "moves",
                        "year_id",
                        "month",
                        "day",
                        "hour_id",
                        "source_type_id",
                        "fuel_type_id",
                        "pollutant_id",
                        "avg_speed_bin_id",
                        "road_type_id",
                        "process_id",
                    ]
                )
                .rate_per_distance.count()
                .var()
            )

            assert (mean_count == 1) & (var_count == 0), (
                "Look for multiple MOVES run in the output. Either filter them "
                "or delete them."
            )
            assert ~running_mvs1.isna().any().any(), "Check for missing " "values."
            mvs_df[key]["df"] = running_mvs1

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
                "road_type_id",
                "road_desc",
                "source_type_id",
                "source_type_name",
                "fuel_type_id",
                "fuel_type_desc",
                "pollutant_id",
                "pollutant_short_name",
                "avg_speed_bin_id",
                "avg_bin_speed",
                "avg_speed_bin_desc",
            ]
        )
        .agg(rate_per_distance=("rate_per_distance", "sum"))
        .reset_index()
    )

    temp_per_diff = final_df_agg.set_index(
        [
            "year_id",
            "month",
            "day",
            "road_type_id",
            "road_desc",
            "source_type_id",
            "source_type_name",
            "fuel_type_id",
            "fuel_type_desc",
            "pollutant_id",
            "pollutant_short_name",
            "avg_speed_bin_id",
            "avg_bin_speed",
            "avg_speed_bin_desc",
            "moves",
        ]
    ).unstack()

    temp_per_diff["per_diff"] = np.round(
        (
            (
                temp_per_diff[("rate_per_distance", "MOVES 3")]
                - temp_per_diff[("rate_per_distance", "MOVES 2014b")]
            )
            / temp_per_diff[("rate_per_distance", "MOVES 2014b")]
        )
        * 100,
        2,
    )

    temp_per_diff = (
        temp_per_diff.reset_index()
        .drop("rate_per_distance", axis=1)
        .droplevel(1, axis=1)
    )
    final_df_agg_1 = final_df_agg.merge(
        temp_per_diff,
        on=[
            "year_id",
            "month",
            "day",
            "road_type_id",
            "road_desc",
            "source_type_id",
            "source_type_name",
            "fuel_type_id",
            "fuel_type_desc",
            "pollutant_id",
            "pollutant_short_name",
            "avg_speed_bin_id",
            "avg_bin_speed",
            "avg_speed_bin_desc",
        ],
    )

    final_df_agg_1.to_csv(out_path)
    final_df_agg_1.to_csv(out_path2)
