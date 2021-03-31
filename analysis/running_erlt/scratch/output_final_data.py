"""
Output final dataset set as csv.
"""

import pandas as pd
import os
from ttierlt.utils import connect_to_server_db, PATH_PROCESSED

path_running_df_final = os.path.join(PATH_PROCESSED, "running_df_final.csv")
conn = connect_to_server_db(database_nm="mvs2014b_erlt_out")
erlt_df_2014b_py = pd.read_sql(
    "SELECT * FROM running_erlt_intermediate_yr_spd_interpolated_no_monthid", conn
)
conn.close()

erlt_df_2014b_py.to_csv(path_running_df_final)
