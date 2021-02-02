"""
Script to batch process extended idling emission rate data and generate a single
database with the final output for different years combined together.
Created by: Apoorba Bibeka
Date Created: 01/29/2021
"""
import os
import logging
import time
import datetime
import functools
import operator
from ttierlt.utils import PATH_INTERIM_EXTNIDLE, get_db_nm_list, connect_to_server_db
from ttierlt.extnidle.extnidle_batch_sql import ExtnidleSqlCmds as erltExtnidle
RERUN_FROM_SCRATCH: bool = False
# FixME: Add a the keyword: "running" at the top. Reuse it across the code. e.g
#  PROCESS=extnidle

if __name__ == "__main__":
    # FixMe: Add the inventory creation to utility module
    if RERUN_FROM_SCRATCH:
        already_processed_db = []
    else:
        # Get already processed db_nm:
        conn = connect_to_server_db(database_nm="mvs2014b_erlt_out")
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT Area, yearid, monthid "
                    "FROM extnidle_erlt_intermediate")
        already_processed_db = cur.fetchall()
        conn.close()
        del conn

    # FixMe: Add the logging setup to utility module.
    # Set logging file details.
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    path_to_log_dir = os.path.join(PATH_INTERIM_EXTNIDLE, "Log Files")
    if not os.path.exists(path_to_log_dir):
        os.mkdir(path_to_log_dir)
    logfilenm = datetime.datetime.now().strftime("extnidle_%H_%M_%d_%m_%Y.log")
    path_log_file = os.path.join(path_to_log_dir, logfilenm)
    logging.basicConfig(filename=path_log_file, filemode="w", level=logging.INFO)
    logfilenm_debug = datetime.datetime.now().strftime(
        "extnidle_debug_%H_%M_%d_%m_%Y.log")
    path_err_log_file = os.path.join(path_to_log_dir, logfilenm_debug)
    logging.basicConfig(filename=path_err_log_file, filemode="w", level=logging.DEBUG)

    # FixMe: Add the getting list of processed databases to utility module.
    # # Get list of processed databases.
    district_abbs = ["elp", "aus", "bmt", "crp", "dal", "ftw", "hou", "wac", "sat"]
    db_nms_list_temp = [
        get_db_nm_list(district_abb=county_abb_) for county_abb_ in district_abbs
    ]
    db_nms_list = functools.reduce(operator.iconcat, db_nms_list_temp, [])

    # Process all databases.
    for db_nm in db_nms_list:
        erlt_extnidle_obj = erltExtnidle(db_nm_=db_nm)
        db_nm_key = (
            erlt_extnidle_obj.area_district, erlt_extnidle_obj.analysis_year,
            erlt_extnidle_obj.anaylsis_month
        )
        if db_nm_key in already_processed_db:
            erlt_extnidle_obj.close_conn()
            del erlt_extnidle_obj
            continue
        start_time = time.time()
        # Run the SQL Commands on the database.
        ########################################################
        logging.info(f"# Start processing {db_nm}")
        print(f"# Start processing {db_nm}")
        print("-------------------------------------------------------------------")
        query_start_time = time.time()
        sample_extnidlerate = erlt_extnidle_obj.aggregate_extnidlerate_rateperhour()
        hourmix_extidle = erlt_extnidle_obj.get_hourmix_extidle()
        txled_elp_dict = erlt_extnidle_obj.get_txled()
        erlt_extnidle_obj.create_indices_before_joins()
        erlt_extnidle_obj.join_extnidlerate_txled_hourmix()
        erlt_extnidle_obj.compute_factored_extnidlerate()
        erlt_extnidle_obj.agg_by_processtype(add_seperate_conflicted_copy=False)
        erlt_extnidle_obj.close_conn()
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
        del erlt_extnidle_obj
