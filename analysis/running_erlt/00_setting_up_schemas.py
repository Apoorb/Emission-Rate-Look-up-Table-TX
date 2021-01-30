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
from ttierlt.movesdb import MovesDb
from ttierlt.running.running_batch_sql import create_running_table_in_db

conn = connect_to_server_db(database_nm=None)
cur = conn.cursor()
# Create Output database if not exist
create_qaqc_output_conflicted_schema()

# Delete the existing output table. It cannot have duplicated data; will raise error if you try to add
# duplicated data.
delete_if_exists = False
if delete_if_exists:
    delete_if_exists_user_input = input(
        "Do you want to loose the data from previous runs and create a fresh running_erlt_intermediate table?(y/n)"
    )
    if delete_if_exists_user_input.lower() == "y":
        create_running_table_in_db(delete_if_exists=delete_if_exists_user_input)

# Clean-up existing intermediate tables.

for db_nm in get_db_nm_list("elp"):
    db_obj = MovesDb(db_nm)
    db_obj.cur.execute(f"DROP TABLE  IF EXISTS TxLed_Long_Copy;")
    db_obj.cur.execute(f"DROP TABLE  IF EXISTS txled_long_{db_obj.district_abb}_{db_obj.analysis_year}")
    db_obj.cur.execute(f"DROP TABLE IF EXISTS hourmix;")
    db_obj.cur.execute(
        f"""
        SELECT * 
        FROM information_schema.tables
        WHERE table_schema = '{db_nm}' 
        AND table_name = 'hourmix_elp'
        LIMIT 1;
    """
    )
    if db_obj.cur.fetchone() != None:
        db_obj.cur.execute(f"RENAME TABLE hourmix_elp TO hourmix_running_elp;")
    db_obj.cur.execute(f"DROP TABLE IF EXISTS vmtmix;")
conn.close()
