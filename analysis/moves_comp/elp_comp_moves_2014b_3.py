"""
Compare El Paso 4 year of running, start, idling, and extnidle emission rates
between MOVES 2014b and 3 by
    hour (9),
    source use type (Combination long haul and passenger car),
    processes
    for NOx and VOC
"""

import pandas as pd
import numpy as np
import os
from ttierlt.utils import connect_to_server_db


if __name__ == "__main__":
    mvs3_df = {
        "elp_2020_7": {"db": "mvs3_erlt_elp_48141_2020_7_cer_out",
                       "df":pd.DataFrame()},
        "elp_2030_7": {"db": "mvs3_erlt_elp_48141_2030_7_cer_out",
                       "df":pd.DataFrame()},
        "elp_2040_7": {"db": "mvs3_erlt_elp_48141_2040_7_cer_out",
                       "df":pd.DataFrame()},
        "elp_2050_7": {"db": "mvs3_erlt_elp_48141_2050_7_cer_out",
                       "df":pd.DataFrame()},
    }

    mvs2014b_df = {
        "elp_2020_7": {"db": "mvs14b_erlt_elp_48141_2020_7_cer_out",
                       "df": pd.DataFrame()},
        "elp_2030_7": {"db": "mvs14b_erlt_elp_48141_2030_7_cer_out",
                       "df": pd.DataFrame()},
        "elp_2040_7": {"db": "mvs14b_erlt_elp_48141_2040_7_cer_out",
                       "df": pd.DataFrame()},
        "elp_2050_7": {"db": "mvs14b_erlt_elp_48141_2050_7_cer_out",
                       "df": pd.DataFrame()},
    }

    for key in mvs3_df.keys():
        conn = connect_to_server_db(database_nm=mvs3_df[key]["db"])
        cur = conn.cursor()
        running_2014b = pd.read_sql(
            f"""SELECT * FROM rateperdistance 
            WHERE hourid = 9 AND pollutantID IN (2, 3, 87)""",
            conn,
        )

        conn.close()

        conn = connect_to_server_db(database_nm=mvs2014b_df[key]["db"])
        cur = conn.cursor()
        conn.close()
