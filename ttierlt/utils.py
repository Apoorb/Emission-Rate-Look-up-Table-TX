import glob
import sys
from pathlib import Path
import os
import mariadb
from dotenv import find_dotenv, load_dotenv
from sqlalchemy import create_engine
import mysql.connector  # Needed for create_engine to work.


def get_project_root() -> Path:
    return Path(__file__).parent.parent


PATH_TO_MARIA_DB_DATA = "C:/ProgramData/MariaDB/MariaDB 10.4/data"
PATH_TO_PROJECT_ROOT = get_project_root()
PATH_INTERIM = os.path.join(PATH_TO_PROJECT_ROOT, "data", "interim")
PATH_RAW = os.path.join(PATH_TO_PROJECT_ROOT, "data", "raw")
PATH_INTERIM_RUNNING = os.path.join(PATH_INTERIM, "running")
if not os.path.exists(PATH_INTERIM_RUNNING):
    os.mkdir(PATH_INTERIM_RUNNING)
PATH_INTERIM_STARTS = os.path.join(PATH_INTERIM, "starts")
if not os.path.exists(PATH_INTERIM_STARTS):
    os.mkdir(PATH_INTERIM_STARTS)
PATH_INTERIM_IDLING = os.path.join(PATH_INTERIM, "idling")
if not os.path.exists(PATH_INTERIM_IDLING):
    os.mkdir(PATH_INTERIM_IDLING)
PATH_INTERIM_EXTNIDLE = os.path.join(PATH_INTERIM, "extnidle")
if not os.path.exists(PATH_INTERIM_EXTNIDLE):
    os.mkdir(PATH_INTERIM_EXTNIDLE)

TEMPLATE_DB_NM = "mvs14b_erlt_elp_48141_2020_1_cer_out"  # Database used for developing the 1st set of SQL queries. It's
# name would be replaced by other database name as we iterate over the different databases.


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


def get_db_nm_list(district_abb="*", db_type="county") -> list:
    """
    Get all the moves database for the county with county code `county_abb` used in the moves output file. By default
    it will return databases for all counties.
    Parameters
    ----------
    district_abb: str
        County abbreviation used while naming the MOVES output database.
    db_type: str
        Database type: county or project
    Returns
    -------
    db_nms_county_year_month: list
        List with database names.
    """
    if db_type.lower() == "county":
        pattern_district_year_month = os.path.join(
            PATH_TO_MARIA_DB_DATA,
            f"mvs14b_erlt_{district_abb}_*_20[0-9][0-9]_[0-9]*_cer_out",
        )
    elif db_type.lower() == "project":
        pattern_district_year_month = os.path.join(
            PATH_TO_MARIA_DB_DATA, f"mvs14b_erlt_{district_abb}_*_20[0-9][0-9]_per_out"
        )
    else:
        raise ValueError("db_type can be either 'project' or 'county'")
    db_dirs_county_year_month = glob.glob(pattern_district_year_month)
    db_nms_county_year_month = [
        os.path.basename(dir_path) for dir_path in db_dirs_county_year_month
    ]
    return db_nms_county_year_month


def create_qaqc_output_conflicted_schema():
    conn = connect_to_server_db(database_nm=None)
    cur = conn.cursor()
    # Create Output database if not exist
    cur.execute("CREATE SCHEMA IF NOT EXISTS mvs2014b_erlt_out;")
    cur.execute("CREATE SCHEMA IF NOT EXISTS mvs2014b_erlt_qaqc;")
    cur.execute("CREATE SCHEMA IF NOT EXISTS mvs2014b_erlt_conflicted;")
    conn.close()


def get_engine_to_output_to_db(out_database):
    """
    Get engine to output data to to out_database using pd.to_sql().
    """
    # find .env automagically by walking up directories until it's found
    dotenv_path = find_dotenv()
    # load up the entries as environment variables
    load_dotenv(dotenv_path)
    host = "127.0.0.1"
    engine = create_engine(
        f"mysql+mysqlconnector://root:{os.environ.get('MARIA_DB_PASSWORD')}@{host}/{out_database}"
    )
    return engine
