"""
Create the Schemas in MaraDB needed for saving TxLED Tables and Output Intermediate data.
Created by: Apoorba Bibeka
Date Created: 01/25/2021
"""
from ttierlt.utils import (
    connect_to_server_db,
    get_db_nm_list,
    create_qaqc_output_conflicted_schema,
)
from ttierlt.running.batch_sql import create_running_table_in_db

conn = connect_to_server_db(database_nm=None)
cur = conn.cursor()
# Create Output database if not exist
create_qaqc_output_conflicted_schema()

# Delete the existing output table. It cannot have duplicated data; will raise error if you try to add
# duplicated data.
delete_running_table_if_exists = input(
    "Do you want to loose the data from previous runs and create a fresh table?(y/n)"
)
delete_if_exists_user_input = False
if delete_running_table_if_exists.lower() == "y":
    print(
        "Change the delete_if_exists_user_input parameter to True. I am intentionally not automating it."
    )
    delete_if_exists_user_input = False
    create_running_table_in_db(delete_if_exists=delete_if_exists_user_input)

# Clean-up existing intermediate tables.
for db_nm in get_db_nm_list("elp"):
    conn = connect_to_server_db(database_nm=db_nm)
    cur = conn.cursor()
    cur.execute(f"DROP TABLE  IF EXISTS TxLed_Long_Copy;")
    cur.execute(f"DROP TABLE IF EXISTS hourmix;")
    cur.execute(f"""
        SELECT * 
        FROM information_schema.tables
        WHERE table_schema = '{db_nm}' 
        AND table_name = 'hourmix_elp'
        LIMIT 1;
    """)
    if cur.fetchone() != None:
        cur.execute(f"RENAME TABLE hourmix_elp TO hourmix_running_elp;")
    cur.execute(f"DROP TABLE IF EXISTS vmtmix;")
conn.close()
