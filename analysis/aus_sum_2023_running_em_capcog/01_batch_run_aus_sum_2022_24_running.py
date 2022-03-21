"""
Get Austin 2023 Summer Running Emissions.
Created by: Apoorba Bibeka
Date Created: 05/23/2021
"""
import re
import time
import mariadb
import pandas as pd

from ttierlt_v1.utils import get_db_nm_list, connect_to_server_db
from ttierlt_v1.running.running_batch_sql import RunningSqlCmds as erltRunning


def create_aus_sum_2022_24_running_table_in_db():
    """
    Create aus_sum_2022_24_running_erlt table for storing output.
    Parameters
    """
    # delete_if_exists: Check if we want to delete the previous stored table
    conn = connect_to_server_db(database_nm=None)
    cur = conn.cursor()
    cur.execute("CREATE SCHEMA IF NOT EXISTS aus_sum_2022_24;")
    cur.execute("DROP TABLE IF EXISTS " "aus_sum_2022_24.aus_sum_2022_24_running_erlt")
    cur.execute(
        """
        CREATE TABLE aus_sum_2022_24.aus_sum_2022_24_running_erlt (
        `Area` CHAR(25) NULL DEFAULT NULL COLLATE 'utf8_unicode_ci',
        `yearid` SMALLINT(5) UNSIGNED NULL DEFAULT NULL,
        `monthid` SMALLINT(5) UNSIGNED NULL DEFAULT NULL,
        `hourid` smallint(5) unsigned DEFAULT NULL,
        `funclass` CHAR(25) NULL DEFAULT NULL COLLATE 'utf8_unicode_ci',
        `avgspeed` FLOAT(3,1) NULL DEFAULT NULL,
        `NH3` DECIMAL(23,19) NULL DEFAULT NULL,
        `PM10_Brakewear` DECIMAL(23,19) NULL DEFAULT NULL,
        `PM10_Tirewear` DECIMAL(23,19) NULL DEFAULT NULL,
        `PM25_Brakewear` DECIMAL(23,19) NULL DEFAULT NULL,
        `PM25_Tirewear` DECIMAL(23,19) NULL DEFAULT NULL,
        `Organic_Carbon` DECIMAL(23,19) NULL DEFAULT NULL,
        `CO2` DECIMAL(23,19) NULL DEFAULT NULL,
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
        CONSTRAINT aus_sum_2022_24_running_erlt PRIMARY KEY (
        Area, yearid, monthid, hourid, funclass, avgspeed)
        )
        COLLATE='utf8_unicode_ci'
        ENGINE=MyISAM;
    """
    )
    conn.close()


def compute_factored_emisrate_by_hour(erlt_running_obj_):
    """Weight the emission rate by vmt for different vehicle types, fuel times,
    proportion of vehicles in different time of day and if the TxLED program is
    active in a county (or majority of county of a district."""
    erlt_running_obj_.cur.execute(
        """UPDATE emisrate SET emisFact = ERate*stypemix*txledfac;"""
    )


def agg_by_rdtype_funcls_avgspd_no_hour(erlt_running_obj_):
    """
    Aggregate (sum) emission rate by Area, yearid, monthid, hourid, funclass,
    avgspeed.
    Insert the aggregated table to
    mvs2014b_erlt_out.running_erlt_intermediate if
    no duplicate exists. Else, ask the user if they want a conflicted copy saved
    in mvs2014_erlt_conflicted schema.
    """
    cmd_insert_agg = """
            INSERT INTO aus_sum_2022_24.aus_sum_2022_24_running_erlt(Area, 
            yearid, monthid, hourid, funclass, avgspeed, NH3, PM10_Brakewear,
            PM10_Tirewear, PM25_Brakewear, PM25_Tirewear, Organic_Carbon, CO2,
            CO, NOX, SO2, NO2, VOC, CO2EQ, PM10, PM25, BENZ, NAPTH, BUTA, FORM, 
            ACTE, ACROL, ETYB, DPM, POM)
            SELECT Area,yearid,monthid,hourid,funclass,avgspeed,
            SUM(IF(pollutantid = 30, emisfact, 0)) AS NH3,
            SUM(IF(pollutantid = 106, emisfact, 0)) AS PM10_Brakewear,
            SUM(IF(pollutantid = 107, emisfact, 0)) AS PM10_Tirewear,
            SUM(IF(pollutantid = 116, emisfact, 0)) AS PM25_Brakewear,
            SUM(IF(pollutantid = 117, emisfact, 0)) AS PM25_Tirewear,
            SUM(IF(pollutantid = 111, emisfact, 0)) AS Organic_Carbon,
            SUM(IF(pollutantid = 90, emisfact, 0)) AS CO2,
            SUM(IF(pollutantid = 2, emisfact, 0)) AS CO,
            SUM(IF(pollutantid = 3, emisfact, 0)) AS NOX,
            SUM(IF(pollutantid = 31, emisfact, 0)) AS SO2,
            SUM(IF(pollutantid = 33, emisfact, 0)) AS NO2,
            SUM(IF(pollutantid = 87, emisfact, 0)) AS VOC,
            SUM(IF(pollutantid = 98, emisfact, 0)) AS CO2EQ,
            SUM(IF(pollutantid = 100, emisfact, 0)) AS PM10,
            SUM(IF(pollutantid = 110, emisfact, 0)) AS PM25,
            SUM(IF(pollutantid = 20, emisfact, 0)) AS BENZ,
            SUM(IF(pollutantid IN (23, 185), emisfact, 0)) AS NAPTH,
            SUM(IF(pollutantid = 24, emisfact, 0)) AS BUTA,
            SUM(IF(pollutantid = 25, emisfact, 0)) AS FORM,
            SUM(IF(pollutantid = 26, emisfact, 0)) AS ACTE,
            SUM(IF(pollutantid = 27, emisfact, 0)) AS ACROL,
            SUM(IF(pollutantid = 41, emisfact, 0)) AS ETYB,
            SUM(IF(pollutantid = 100 AND fueltypeid = 2, emisfact, 0)) AS DPM,
            SUM(IF(pollutantid IN (68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78,
            81, 82, 83, 84, 168, 169, 170, 171, 172, 173, 174, 175, 176, 177,
            178, 181, 182, 183, 184), emisfact, 0)) AS POM
            FROM emisrate
            GROUP BY Area,yearid,monthid,hourid,funclass,avgspeed
        
    """
    erlt_running_obj_.cur.execute(cmd_insert_agg)


if __name__ == "__main__":
    create_aus_sum_2022_24_running_table_in_db()
    db_nms_list_temp = get_db_nm_list(district_abb="aus")
    aus_sum_2022_2024_pat = re.compile("mvs14b_erlt_aus_48453_202[24]_7_cer_out")
    db_aus_sum_2022_2024 = [
        db for db in db_nms_list_temp if re.match(aus_sum_2022_2024_pat, db)
    ]
    db_nm = db_aus_sum_2022_2024[0]
    for db_nm in db_aus_sum_2022_2024:
        erlt_running_obj = erltRunning(db_nm_=db_nm)
        erlt_running_obj.aggregate_emisrate_rateperdist()
        hourmix = erlt_running_obj.get_hourmix()
        vmt_mix = erlt_running_obj.get_vmtmix()
        txled_dict = erlt_running_obj.get_txled()
        erlt_running_obj.create_indices_before_joins()
        erlt_running_obj.join_emisrate_vmt_tod_txled()
        compute_factored_emisrate_by_hour(erlt_running_obj)
        agg_by_rdtype_funcls_avgspd_no_hour(erlt_running_obj)
