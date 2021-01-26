"""
Script to batch process EL-Passo emission rate data and generate a single database with the final output for different
years combined together.
Created by: Apoorba Bibeka
Date Created: 01/22/2021
"""
import os
from dotenv import load_dotenv, find_dotenv
import time
from src.utils import MAP_COUNTY_ABB_FULL_NM, connect_to_server_db, get_db_nm_county_year_dict
from src.utils import PATH_TO_PROJECT_ROOT
from src.utils import PATH_TO_ERLT_FILES
from src.utils import TEMPLATE_DB_NM
import re
from sqlalchemy import create_engine
import pandas as pd

DEBUG = False

if __name__ == "__main__":
    # Get Cursor
    db_nms_county_year_month_dict = get_db_nm_county_year_dict(county_abb="elp")
    list_erlt_dfs = []
    STOP_ITER = 1
    ITER_CNTER = 0
    for db_nm, db_county_year_month in db_nms_county_year_month_dict.items():
        start_time = time.time()
        print(f"Processing {db_nm}")
        if DEBUG & ITER_CNTER == STOP_ITER:
            break
        ITER_CNTER = ITER_CNTER + 1
        conn = connect_to_server_db(db_nm)
        cur = conn.cursor()
        # Read SQL Commands.
        sql_command_file = os.path.join(
            PATH_TO_PROJECT_ROOT, "src/batch_process/Python_Input_SQL_Commands_ELP_2020_1.sql"
        )
        sql_command_file_rd_obj = open(sql_command_file, "r").read()
        sql_commands = sql_command_file_rd_obj.split(";")
        # Had issue with last line having empty values like " ", "", or "\n". Using the following regex expression to screen
        # the last line.
        invalid_text_commands = re.compile(r"\s*")
        # Iterate over all the SQL commands.
        for command in sql_commands:
            if re.fullmatch(invalid_text_commands, command):
                if DEBUG:
                    ("Bad Command: ", command)
                continue
            query_start_time = time.time()
            command_current_db = command.replace(TEMPLATE_DB_NM, db_nm)
            district_nm = MAP_COUNTY_ABB_FULL_NM[db_county_year_month["county"]]
            command_current_db_district_nm = command_current_db.replace(
                "Placeholder_District_Name", district_nm
            )
            if DEBUG:
                (command_current_db_district_nm)
            cur.execute(command_current_db_district_nm)
            if DEBUG:
                print(
                    "---Query execution time:  %s seconds ---"
                    % (time.time() - query_start_time)
                )
        print(
            f"---{db_nm} query execution time:  %s seconds ---"
            % (time.time() - start_time)
        )
        temp_erlt_df = pd.read_sql("SELECT * FROM MVS2014b_ERLT.temp_erlt_table", conn)
        out_file_name = "erlt_" + "_".join(db_county_year_month.values()) + ".csv"
        out_file_full_path = os.path.join(PATH_TO_ERLT_FILES, out_file_name)
        temp_erlt_df.to_csv(out_file_full_path, index=False)

        list_erlt_dfs.append(temp_erlt_df)
        conn.close()

    final_erlt_df = pd.concat(list_erlt_dfs)
    # find .env automagically by walking up directories until it's found
    dotenv_path = find_dotenv()
    # load up the entries as environment variables
    load_dotenv(dotenv_path)
    host = "127.0.0.1"
    out_database = "MVS2014b_ERLT_OUT"
    engine = create_engine(
        f"mysql+mysqlconnector://root:{os.environ.get('MARIA_DB_PASSWORD')}@{host}/{out_database}"
    )
    final_erlt_df.to_sql("El_Paso_ERLT", con=engine, if_exists="replace", index=False)
