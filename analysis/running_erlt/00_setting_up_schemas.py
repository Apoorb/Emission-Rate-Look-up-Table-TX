"""
Create the Schemas in MaraDB needed for saving TxLED Tables and Output Intermediate data.
Created by: Apoorba Bibeka
Date Created: 01/25/2021
"""
from ttierlt.utils import connect_to_server_db, get_db_nm_list, create_qaqc_output_conflicted_schema


conn = connect_to_server_db(database_nm=None)
cur = conn.cursor()
# Create Output database if not exist
create_qaqc_output_conflicted_schema()

# Clean-up existing intermediate tables.
for db_nm in get_db_nm_list("elp"):
    conn = connect_to_server_db(database_nm=db_nm)
    cur = conn.cursor()
    cur.execute(f"DROP TABLE  IF EXISTS TxLed_Long_Copy;")
    cur.execute(f"DROP TABLE IF EXISTS hourmix;")
    cur.execute(f"DROP TABLE IF EXISTS vmtmix;")
conn.close()

