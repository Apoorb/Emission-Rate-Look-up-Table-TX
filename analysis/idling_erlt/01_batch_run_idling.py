"""
Script to batch process idling emission rate data and generate a single database with
the final output for different years combined together.
Created by: Apoorba Bibeka
Date Created: 01/29/2021
"""
import os
import logging
import time
import datetime
import functools
import operator
from ttierlt_v1.utils import PATH_INTERIM_IDLING, get_db_nm_list, connect_to_server_db
from ttierlt_v1.idling.idling_batch_sql import IdlingSqlCmds as erltIdling

RERUN_FROM_SCRATCH: bool = False
# FixME: Add a the keyword: "running" at the top. Reuse it across the code. e.g
#  PROCESS=idling

if __name__ == "__main__":
    # FixMe: Add the inventory creation to utility module
    if RERUN_FROM_SCRATCH:
        already_processed_db = []
    else:
        # Get already processed db_nm:
        conn = connect_to_server_db(database_nm="mvs2014b_erlt_out")
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT Area, yearid" " FROM idling_erlt_intermediate")
        already_processed_db = cur.fetchall()
        conn.close()
        del conn

    # FixMe: Add the logging setup to utility module.
    # Set logging file details.
    path_to_log_dir = os.path.join(PATH_INTERIM_IDLING, "Log Files")
    if not os.path.exists(path_to_log_dir):
        os.mkdir(path_to_log_dir)
    logfilenm = datetime.datetime.now().strftime("starts_%H_%M_%d_%m_%Y.log")
    path_log_file = os.path.join(path_to_log_dir, logfilenm)
    logging.basicConfig(filename=path_log_file, filemode="w", level=logging.INFO)

    # FixMe: Add the getting list of processed databases to utility module.
    # # Get list of processed databases.
    district_abbs = ["elp", "aus", "bmt", "crp", "dal", "ftw", "hou", "wac", "sat"]
    db_nms_list_temp = [
        get_db_nm_list(district_abb=district_abb, db_type="project")
        for district_abb in district_abbs
    ]
    db_nms_list = functools.reduce(operator.iconcat, db_nms_list_temp, [])

    for db_nm in db_nms_list:
        erlt_idling_obj = erltIdling(db_nm_=db_nm)
        db_nm_key = (erlt_idling_obj.area_district, erlt_idling_obj.analysis_year)
        if db_nm_key in already_processed_db:
            erlt_idling_obj.close_conn()
            del erlt_idling_obj
            continue

        start_time = time.time()
        # Run the SQL Commands on the database.
        ########################################################
        logging.info(f"# Start processing {db_nm}")
        print(f"# Start processing {db_nm}")
        print("-------------------------------------------------------------------")
        query_start_time = time.time()
        head_idlerate_df = erlt_idling_obj.aggregate_idlerate_movesoutput()
        hourmix_idling = erlt_idling_obj.get_houridlemix()
        sutmix_idling = erlt_idling_obj.get_sutmix()
        txled_elp_dict = erlt_idling_obj.get_txled()
        erlt_idling_obj.create_indices_before_joins()
        erlt_idling_obj.join_idlerate_houridlemix_sutmix_txled()
        erlt_idling_obj.compute_factored_emisrate()
        erlt_idling_obj.agg_by_hourid_period(add_seperate_conflicted_copy=False)
        erlt_idling_obj.close_conn()
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
        del erlt_idling_obj
