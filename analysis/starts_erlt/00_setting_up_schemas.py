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
from ttierlt.starts.starts_batch_sql import create_starts_table_in_db

conn = connect_to_server_db(database_nm=None)
cur = conn.cursor()
# Create Output database if not exist
create_qaqc_output_conflicted_schema()

# Delete the existing output table. It cannot have duplicated data; will raise error if you try to add
# duplicated data.
delete_if_exists = True
if delete_if_exists:
    delete_if_exists_user_input = input(
        "Do you want to loose the data from previous runs and create a fresh starts_erlt_intermediate table?(y/n)"
    )
    if delete_if_exists_user_input.lower() == "y":
        create_starts_table_in_db(delete_if_exists=delete_if_exists_user_input)
