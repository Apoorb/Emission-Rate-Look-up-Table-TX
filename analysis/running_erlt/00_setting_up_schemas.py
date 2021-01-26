"""
Create the Schemas in MaraDB needed for saving TxLED Tables and Output Intermediate data.
Created by: Apoorba Bibeka
Date Created: 01/25/2021
"""
from ttierlt.utils import connect_to_server_db, get_db_nm_list


conn = connect_to_server_db(database_nm=None)
cur = conn.cursor()
# Create Output database if not exist
cur.execute("CREATE SCHEMA IF NOT EXISTS mvs2014b_erlt_out;")
cur.execute("CREATE SCHEMA IF NOT EXISTS mvs2014b_erlt_qaqc;")

# Check if we want to delete the previous stored table
DELETE_DATA = input("Do you want to delete the data in mvs2014b_erlt_out.running_erlt_intermediate.: (y/n")
if DELETE_DATA.lower() == "y": cur.execute("DROP TABLE  IF EXISTS mvs2014b_erlt_out.running_erlt_intermediate")
cur.execute("""
    CREATE TABLE mvs2014b_erlt_out.running_erlt_intermediate (
        `Area` CHAR(25) NULL DEFAULT NULL COLLATE 'utf8_unicode_ci',
        `yearid` SMALLINT(5) UNSIGNED NULL DEFAULT NULL,
        `monthid` SMALLINT(5) UNSIGNED NULL DEFAULT NULL,
        `funclass` CHAR(25) NULL DEFAULT NULL COLLATE 'utf8_unicode_ci',
        `avgspeed` FLOAT(3,1) NULL DEFAULT NULL,
        `CO` DECIMAL(23,19) NULL DEFAULT NULL,
        `NOX` DECIMAL(23,19) NULL DEFAULT NULL,
        `SO2` DECIMAL(23,19) NULL DEFAULT NULL,
        `NO2` DECIMAL(23,19) NULL DEFAULT NULL,
        `VOC` DECIMAL(23,19) NULL DEFAULT NULL,
        `CO2EQ` DECIMAL(23,19) NULL DEFAULT NULL,
        `PM10` DECIMAL(23,19) NULL DEFAULT NULL,
        `PM25` DECIMAL(23,19) NULL DEFAULT NULL,
        `BENZ` DECIMAL(23,19) NULL DEFAULT NULL,
        `NAPTH` DECIMAL(23,19) NULL DEFAULT NULL,
        `BUTA` DECIMAL(23,19) NULL DEFAULT NULL,
        `FORM` DECIMAL(23,19) NULL DEFAULT NULL,
        `ACTE` DECIMAL(23,19) NULL DEFAULT NULL,
        `ACROL` DECIMAL(23,19) NULL DEFAULT NULL,
        `ETYB` DECIMAL(23,19) NULL DEFAULT NULL,
        `DPM` DECIMAL(23,19) NULL DEFAULT NULL,
        `POM` DECIMAL(23,19) NULL DEFAULT NULL,
        CONSTRAINT running_erlt_intermediate_pk PRIMARY KEY (Area, yearid, monthid, funclass, avgspeed)
    )
    COLLATE='utf8_unicode_ci'
    ENGINE=MyISAM;
""")
conn.close()

# Clean-up existing intermediate tables.
for db_nm in get_db_nm_list("elp"):
    conn = connect_to_server_db(database_nm=db_nm)
    cur = conn.cursor()
    # cur.execute(f"DROP TABLE  IF EXISTS emisrate;")
    cur.execute(f"DROP TABLE IF EXISTS hourmix;")
    cur.execute(f"DROP TABLE IF EXISTS vmtmix;")
conn.close()

