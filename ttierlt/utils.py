import glob
import sys
from pathlib import Path
import os
import mariadb
from dotenv import find_dotenv, load_dotenv


def get_project_root() -> Path:
    return Path(__file__).parent.parent


PATH_TO_MARIA_DB_DATA = "C:/ProgramData/MariaDB/MariaDB 10.4/data"
PATH_TO_PROJECT_ROOT = get_project_root()
PATH_INTERIM = os.path.join(PATH_TO_PROJECT_ROOT, "data", "interim")
PATH_RAW = os.path.join(PATH_TO_PROJECT_ROOT, "data", "raw")
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


def get_db_nm_list(county_abb="*") -> list:
    """
    Get all the moves database for the county with county code `county_abb` used in the moves output file. By default
    it will return databases for all counties.
    Parameters
    ----------
    county_abb: str
        County abbreviation used while naming the MOVES output database.
    Returns
    -------
    db_nms_county_year_month: list
        List with database names.
    """
    pattern_county_year_month = os.path.join(
        PATH_TO_MARIA_DB_DATA, f"mvs14b_erlt_{county_abb}_*_20[0-9][0-9]_[0-9]*_cer_out"
    )
    db_dirs_county_year_month = glob.glob(pattern_county_year_month)
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

