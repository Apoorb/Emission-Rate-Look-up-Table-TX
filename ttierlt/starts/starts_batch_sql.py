"""
Module to execute SQL commands for starts emission process.
Created by: Apoorba Bibeka
Created on: 01/29/2021
"""
import time
import pandas as pd
import mariadb
import os
import datetime
import logging
from ttierlt.movesdb import MovesDb
from ttierlt.utils import connect_to_server_db, get_db_nm_list, PATH_INTERIM_STARTS


def create_starts_table_in_db(delete_if_exists=False):
    """
    Create  mvs2014b_erlt_out.running_erlt_intermediate table for storing output.
    Parameters
    ----------
    delete_if_exists: Delete the existing mvs2014b_erlt_out.running_erlt_intermediate table (if it exists).
    """
    # delete_if_exists: Check if we want to delete the previous stored table
    conn = connect_to_server_db(database_nm=None)
    cur = conn.cursor()
    if delete_if_exists:
        cur.execute("DROP TABLE  IF EXISTS mvs2014b_erlt_out.starts_erlt_intermediate")
    cur.execute(
        """
        CREATE TABLE mvs2014b_erlt_out.starts_erlt_intermediate (
            `Area` CHAR(25) NULL DEFAULT NULL COLLATE 'utf8_unicode_ci',
            `yearid` SMALLINT(5) UNSIGNED NULL DEFAULT NULL,
            `monthid` SMALLINT(5) UNSIGNED NULL DEFAULT NULL,
---------------------------------------------------------            `funclass` CHAR(25) NULL DEFAULT NULL COLLATE 'utf8_unicode_ci',
---------------------------------------------------------            `avgspeed` FLOAT(3,1) NULL DEFAULT NULL,
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
            CONSTRAINT running_erlt_intermediate_pk PRIMARY KEY (Area, yearid, monthid, vehicletype, fueltype)
        )
        COLLATE='utf8_unicode_ci'
        ENGINE=MyISAM;
    """
    )
    conn.close()


class StartSqlCmds(MovesDb):
    """
    Function to execute SQL commands for starts emission process.
    """

    def __init__(self, db_nm_, county_abb_):
        super().__init__(
            db_nm_ =db_nm_,
            county_abb_=county_abb_
        )
        self.head_startrate_df = pd.DataFrame()
        self.hourmix_starts = pd.DataFrame()
        self.vmtmix = pd.DataFrame()
        self.txled = pd.DataFrame()
        self.created_all_indices = False

    def aggregate_startrate_rateperstart(self):
        pass

    def _update_startrate_rateperstart(self):
        pass

    def get_hourmix_starts(self):
        pass

    def _update_hourmix_starts(self):
        pass

    def create_indices_before_joins(self):
        pass

    def join_startrate_hourmix(self):
        pass

    def compute_factored_startrate(self):
        pass

    def agg_by_vehtyp_fueltyp(self, add_seperate_conflicted_copy=False, conflicted_copy_suffix=""):
        pass

if __name__ == "__main__":
    path_to_log_dir = os.path.join(PATH_INTERIM_STARTS, "Log Files")
    if not os.path.exists(path_to_log_dir):
        os.mkdir(path_to_log_dir)
    path_log_file = os.path.join(path_to_log_dir, "starts_test_sql.log")
    logging.basicConfig(filename=path_log_file, filemode="w", level=logging.INFO)
    # ---
    db_nms_list = get_db_nm_list(county_abb="elp")
    db_nm = "mvs14b_erlt_elp_48141_2022_7_cer_out"
    logging.info(f"# Start processing {db_nm}")
    elp_2022_7_obj = StartSqlCmds(db_nm_=db_nm, county_abb_="elp")
    query_start_time = time.time()
    elp_2022_7_obj.aggregate_startrate_rateperstart()
    hourmix_starts = elp_2022_7_obj.get_hourmix_starts()
    txled_elp_dict = elp_2022_7_obj.get_txled_for_db_district_year()
    elp_2022_7_obj.create_indices_before_joins()
    elp_2022_7_obj.join_startrate_hourmix()
    elp_2022_7_obj.compute_factored_startrate()
    elp_2022_7_obj.agg_by_vehtyp_fueltyp(
        add_seperate_conflicted_copy=True,
        conflicted_copy_suffix="drop_after_testing"
    )
    elp_2022_7_obj.close_conn()
    logging.info(
        "---Query execution time:  %s seconds ---"
        % (time.time() - query_start_time)
    )
    logging.info(f"# End processing {db_nm}")
    del elp_2022_7_obj
