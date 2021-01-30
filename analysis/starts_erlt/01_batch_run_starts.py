"""
Script to batch process starts emission rate data and generate a single database with the final output for different
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
from ttierlt.utils import PATH_INTERIM_STARTS, get_db_nm_list
from ttierlt.starts.starts_batch_sql import StartSqlCmds as erltStarts
from ttierlt.movesdb import MovesDb


if __name__ == "__main__":
    # Set logging file details.
    path_to_log_dir = os.path.join(PATH_INTERIM_STARTS, "Log Files")
    if not os.path.exists(path_to_log_dir):
        os.mkdir(path_to_log_dir)
    logfilenm = datetime.datetime.now().strftime("starts_%H_%M_%d_%m_%Y.log")
    path_log_file = os.path.join(path_to_log_dir, logfilenm)
    logging.basicConfig(filename=path_log_file, filemode="w", level=logging.INFO)
    # # Get list of processed databases.
    # TODO: Inventory and skip processed files.

    county_abbs = ["aus", "bmt", "crp", "dal", "ftw", "hou", "wac", "sat"]
    db_nms_list_temp = [get_db_nm_list(county_abb=county_abb_) for county_abb_ in county_abbs]
    db_nms_list = functools.reduce(operator.iconcat, db_nms_list_temp, [])

    for db_nm in db_nms_list:
        start_time = time.time()
        # Run the SQL Commands on the database.
        ########################################################
        logging.info(f"# Start processing {db_nm}")
        print(f"# Start processing {db_nm}")
        print("-------------------------------------------------------------------")
        erlt_starts_obj = erltStarts(db_nm_=db_nm)
        query_start_time = time.time()
        query_start_time = time.time()
        sample_startrate = erlt_starts_obj.aggregate_startrate_rateperstart()
        hourmix_starts = erlt_starts_obj.get_hourmix_starts()
        txled_elp_dict = erlt_starts_obj.get_txled_for_db_district_year()
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
            "------------------------------------------------------------------------------------------"
        )
        erlt_starts_obj.close_conn()
        del erlt_starts_obj
