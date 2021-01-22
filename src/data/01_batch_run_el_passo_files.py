import os
from dotenv import load_dotenv, find_dotenv
import mariadb # https://mariadb.com/resources/blog/how-to-connect-python-programs-to-mariadb/
import sys
import pandas as pd

# find .env automagically by walking up directories until it's found
dotenv_path = find_dotenv()

# load up the entries as environment variables
load_dotenv(dotenv_path)

# Connect to MariaDB Platform
try:
    conn = mariadb.connect(
        user="root",
        password=os.environ.get("MARIA_DB_PASSWORD"),
        host="127.0.0.1",
        port=3306,
        database="mvs14b_erlt_elp_48141_2020_1_cer_out"
    )
except mariadb.Error as e:
    print(f"Error connecting to MariaDB Platform: {e}")
    sys.exit(1)

# Get Cursor
cur = conn.cursor()
cur.execute("SHOW TABLES")
tables = cur.fetchall()
