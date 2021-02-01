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
from ttierlt.utils import PATH_INTERIM_EXTNIDLE, get_db_nm_list
from ttierlt.extnidle.extnidle_batch_sql import ExtnidleSqlCmds as erltExtnidle


if __name__ == "__main__":
    # Set logging file details.
    path_to_log_dir = os.path.join(PATH_INTERIM_EXTNIDLE, "Log Files")
    if not os.path.exists(path_to_log_dir):
        os.mkdir(path_to_log_dir)
    logfilenm = datetime.datetime.now().strftime("starts_%H_%M_%d_%m_%Y.log")
    path_log_file = os.path.join(path_to_log_dir, logfilenm)
    logging.basicConfig(filename=path_log_file, filemode="w", level=logging.INFO)
    # # Get list of processed databases.
    # TODO: Inventory and skip processed files.
    county_abbs = ["elp", "aus", "bmt", "crp", "dal", "ftw", "hou", "wac", "sat"]
    db_nms_list_temp = [
        get_db_nm_list(district_abb=county_abb_) for county_abb_ in county_abbs
    ]
    db_nms_list = functools.reduce(operator.iconcat, db_nms_list_temp, [])

    for db_nm in db_nms_list:
        start_time = time.time()
        # Run the SQL Commands on the database.
        ########################################################
        logging.info(f"# Start processing {db_nm}")
        print(f"# Start processing {db_nm}")
        print("-------------------------------------------------------------------")
        query_start_time = time.time()
        erlt_extnidle_obj = erltExtnidle(db_nm_=db_nm)
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
