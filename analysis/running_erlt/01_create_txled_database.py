import os
import mysql.connector # Needed for create_engine to work.
from dotenv import load_dotenv, find_dotenv
from sqlalchemy import create_engine
import pandas as pd
from ttierlt.utils import PATH_RAW, PATH_INTERIM, connect_to_server_db

# Read in a sample database to get the sourcetypeid and the fueltypeid.
conn = connect_to_server_db(database_nm="mvs14b_erlt_elp_48141_2020_1_cer_out")
unique_pollutant_year_sourcetype_fueltype = pd.read_sql(
    "SELECT DISTINCT yearid, pollutantid, sourcetypeid, fueltypeid FROM rateperdistance", conn
)
# Read in the TxLED data and Convert it into Long Format.
path_to_txled = os.path.join(PATH_RAW, "ERLT-TxLED Factor Summary.xlsx")
txled_data = pd.read_excel(path_to_txled)
col_yr_rename_dict = {col: f"yr{col}" for col in txled_data.columns if type(col) == int}
txled_data_yr_renamed = txled_data.rename(columns=col_yr_rename_dict)
txled_long = (
    pd.wide_to_long(txled_data_yr_renamed, stubnames='yr', i=['Source Use Type (SUT)', 'SUT ID'], j='yearid')
    .reset_index()
    .drop(columns="Source Use Type (SUT)")
    .rename(columns={'SUT ID': 'sourcetypeid', "yr": "txled_fac"})
    .assign(
        fueltypeid=2, # TxLED is factored for Diesel vehicles only.
        pollutantid_NOX=3,
        pollutantid_NO2=33,
    )
    .pipe(pd.wide_to_long,
          stubnames="pollutantid", i=["yearid", "sourcetypeid", "fueltypeid"], j="pollutantnm", sep="_", suffix=r"\w{3}")
    .reset_index()
    .filter(items=["yearid", "pollutantid", "sourcetypeid", "fueltypeid", "txled_fac"])
)
path_out_txled_long = os.path.join(PATH_INTERIM, "txled_long.csv")
txled_long.to_csv(path_out_txled_long)
conn.close()
conn = connect_to_server_db(database_nm=None)
cur = conn.cursor()
cur.execute("CREATE DATABASE IF NOT EXISTS txled_db;")
# find .env automagically by walking up directories until it's found
dotenv_path = find_dotenv()
# load up the entries as environment variables
load_dotenv(dotenv_path)
host = "127.0.0.1"
out_database = "txled_db"
engine = create_engine(
    f"mysql+mysqlconnector://root:{os.environ.get('MARIA_DB_PASSWORD')}@{host}/{out_database}"
)
txled_long.to_sql("txled_long", con=engine, if_exists="replace", index=False)


