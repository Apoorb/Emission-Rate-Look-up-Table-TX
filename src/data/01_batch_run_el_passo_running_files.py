"""
Script to batch process EL-Passo emission rate data and generate a single database with the final output for different
years combined togather.
"""
import os
from dotenv import load_dotenv, find_dotenv
import mariadb  # https://mariadb.com/resources/blog/how-to-connect-python-programs-to-mariadb/
import sys
import time
import glob
from src.utils import get_project_root
import re
from sqlalchemy import create_engine
import mysql.connector
import pandas as pd

# GLOBAL PATHS
DEBUG = False
MAP_COUNTY_ABB_FULL_NM = {"elp": "El Paso"}
PATH_TO_MARIA_DB_DATA = "C:/ProgramData/MariaDB/MariaDB 10.4/data"
PATH_TO_PROJECT_ROOT = get_project_root()
PATH_INTERIM = os.path.join(PATH_TO_PROJECT_ROOT, "data", "interim")
PATH_TO_ERLT_FILES = os.path.join(PATH_INTERIM, "ERLT Files")
if not os.path.exists(PATH_TO_ERLT_FILES):
    os.mkdir(PATH_TO_ERLT_FILES)
TEMPLATE_DB_NM = "mvs14b_erlt_elp_48141_2020_1_cer_out"  # Database used for developing the 1st set of SQL queries. It's
# name would be replaced by other database name as we iterate over the different databases.


def get_db_nm_county_year_dict(county_abb) -> dict:
    """
    Get all the moves database for the county with county code `county_abb` used in the moves output file. Output a
    dict with database name and the respective county, year, and month name.
    Example: mvs14b_erlt_elp_48141_2020_1_cer_out can be decomposed as follows:
        mvs14b: MOVES 2014---index 0
        erlt: Project name; emission rate look-up table---index 1
        elp: El-Passo---index 2
        48141: FIPS code for El-Passo County---index 3
        2020: Year---index 4
        1: Month; Jan---index 5
        cer: Garbage
        out: Garbage
    Parameters
    ----------
    county_abb: str
        County abbreviation used while naming the MOVES output database.
    Returns
    -------
    db_nms_county_year_month_dict_: dict
        Dictionary with database name as the key and another dictionary with county, FIPS, year, and month ID as value.
    """
    pattern_county_year_month = os.path.join(
        PATH_TO_MARIA_DB_DATA, f"mvs14b_erlt_{county_abb}_*_20[0-9][0-9]_[0-9]*_cer_out"
    )
    db_dirs_county_year_month = glob.glob(pattern_county_year_month)
    db_nms_county_year_month = [
        os.path.basename(dir_path) for dir_path in db_dirs_county_year_month
    ]
    db_nms_county_year_month_dict_ = {
        db_nm: {
            "county": db_nm.split("_")[2],
            "fips": db_nm.split("_")[3],
            "year": db_nm.split("_")[4],
            "month_id": db_nm.split("_")[5],
        }
        for db_nm in db_nms_county_year_month
    }
    return db_nms_county_year_month_dict_


def connect_to_server_db(database_nm):
    """
    Function to connect to a particular database on the server.
    Returns
    -------
    conn_: mariadb.connection
        Connection object to access the data in MariaDB Server.
    """
    # find .env automagically by walking up directories until it's found
    dotenv_path = find_dotenv()
    # load up the entries as environment variables
    load_dotenv(dotenv_path)
    # Connect to MariaDB Platform
    try:
        conn_ = mariadb.connect(
            user="root",
            password=os.environ.get("MARIA_DB_PASSWORD"),
            host="127.0.0.1",
            port=3306,
            database=database_nm,
        )
    except mariadb.Error as e:
        print(f"Error connecting to MariaDB Platform: {e}")
        sys.exit(1)
    return conn_


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
            PATH_TO_PROJECT_ROOT, "src/data/Python_Input_SQL_Commands_ELP_2020_1.sql"
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
    engine = create_engine(
        "mysql+mysqlconnector://root:civil123@127.0.0.1/MVS2014b_ERLT_OUT"
    )
    final_erlt_df.to_sql("El_Paso_ERLT", con=engine, if_exists="replace", index=False)
