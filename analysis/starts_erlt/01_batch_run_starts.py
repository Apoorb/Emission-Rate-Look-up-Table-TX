"""
Script to batch process starts emission rate data and generate a single database with
the final output for different
years combined together.
Created by: Apoorba Bibeka
Date Created: 01/29/2021
"""
import os
import logging
import time
import datetime
import functools
import operator
from ttierlt.utils import PATH_INTERIM_STARTS, get_db_nm_list, connect_to_server_db
from ttierlt.starts.starts_batch_sql import StartSqlCmds as erltStarts

RERUN_FROM_SCRATCH: bool = False

if __name__ == "__main__":
    # FixMe: Add the inventory creation to utility module
    if RERUN_FROM_SCRATCH:
        already_processed_db = []
    else:
        # Get already processed db_nm:
        conn = connect_to_server_db(database_nm="mvs2014b_erlt_out")
        cur = conn.cursor()
        cur.execute(
            "SELECT DISTINCT Area, yearid, monthid " "FROM starts_erlt_intermediate"
        )
        already_processed_db = cur.fetchall()
        conn.close()
        del conn

    # FixMe: Add the logging setup to utility module.
    # Set logging file details.
    path_to_log_dir = os.path.join(PATH_INTERIM_STARTS, "Log Files")
    if not os.path.exists(path_to_log_dir):
        os.mkdir(path_to_log_dir)
    logfilenm = datetime.datetime.now().strftime("starts_%H_%M_%d_%m_%Y.log")
    path_log_file = os.path.join(path_to_log_dir, logfilenm)
    logging.basicConfig(filename=path_log_file, filemode="w", level=logging.INFO)

    # FixMe: Add the getting list of processed databases to utility module.
    # # Get list of processed databases.
    district_abbs = ["elp", "aus", "bmt", "crp", "dal", "ftw", "hou", "wac", "sat"]
    db_nms_list_temp = [
        get_db_nm_list(district_abb=county_abb_) for county_abb_ in district_abbs
    ]
    db_nms_list = functools.reduce(operator.iconcat, db_nms_list_temp, [])

    for db_nm in db_nms_list:
        # Skip processed databases:
        ########################################################
        erlt_starts_obj = erltStarts(db_nm_=db_nm)
        db_nm_key = (
            erlt_starts_obj.area_district,
            erlt_starts_obj.analysis_year,
            erlt_starts_obj.anaylsis_month,
        )
        if db_nm_key in already_processed_db:
            erlt_starts_obj.close_conn()
            del erlt_starts_obj
            continue
        start_time = time.time()
        # Run the SQL Commands on the database.
        ########################################################
        logging.info(f"# Start processing {db_nm}")
        print(f"# Start processing {db_nm}")
        print("-------------------------------------------------------------------")
        query_start_time = time.time()
        erlt_starts_obj = erltStarts(db_nm_=db_nm)
        sample_startrate = erlt_starts_obj.aggregate_startrate_rateperstart()
        hourmix_starts = erlt_starts_obj.get_hourmix_starts()
        txled_elp_dict = erlt_starts_obj.get_txled()
        erlt_starts_obj.create_indices_before_joins()
        erlt_starts_obj.join_startrate_txled_hourmix()
        erlt_starts_obj.compute_factored_startrate()
        erlt_starts_obj.agg_by_vehtyp_fueltyp(
            add_seperate_conflicted_copy=False,
            conflicted_copy_suffix="drop_after_testing",
        )
        logging.info(
            "---Query execution time:  %s seconds ---"
            % (time.time() - query_start_time)
        )
        logging.info(f"# End processing {db_nm}")
        print(
            "---Query execution time:  %s seconds ---"
            % (time.time() - query_start_time)
        )
        print(f"# End processing {db_nm}")
        print(
            "--------------------------------------------------------------------------"
        )
        erlt_starts_obj.close_conn()
        del erlt_starts_obj
