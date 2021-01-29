"""
Script to batch process EL-Passo emission rate data and generate a single database with the final output for different
years combined together.
Created by: Apoorba Bibeka
Date Created: 01/22/2021
"""
import os
import logging
import time
import datetime
from ttierlt.utils import PATH_INTERIM_RUNNING, get_db_nm_list
from ttierlt.running.running_batch_sql import RunningSqlCmds as erltRunning


if __name__ == "__main__":
    # Set logging file details.
    path_to_log_dir = os.path.join(PATH_INTERIM_RUNNING, "Log Files")
    if not os.path.exists(path_to_log_dir):
        os.mkdir(path_to_log_dir)
    logfilenm = datetime.datetime.now().strftime(
        "running_%H_%M_%d_%m_%Y.log"
    )
    path_log_file = os.path.join(path_to_log_dir, logfilenm)
    logging.basicConfig(filename=path_log_file, filemode="w", level=logging.INFO)
    # # Get list of processed databases.
    # TODO: Inventory and skip processed files.

    db_nms_list = get_db_nm_list(county_abb="elp")
    for db_nm in db_nms_list:
        start_time = time.time()
        # Run the SQL Commands on the database.
        ########################################################
        logging.info(f"# Start processing {db_nm}")
        print(f"# Start processing {db_nm}")
        print("-------------------------------------------------------------------")
        erlt_running_obj = erltRunning(db_nm_=db_nm, county_abb_="elp")
        query_start_time = time.time()
        erlt_running_obj.aggregate_emisrate_rateperdist()
        hourmix_elp = erlt_running_obj.get_hourmix_for_db_district()
        vmt_mix_elp_2022 = (
            erlt_running_obj.get_vmtmix_for_db_district_weekday_closest_vmt_yr()
        )
        txled_elp_dict = erlt_running_obj.get_txled_for_db_district_year()
        erlt_running_obj.create_indices_before_joins()
        erlt_running_obj.join_emisrate_vmt_tod_txled()
        erlt_running_obj.compute_factored_emisrate()
        erlt_running_obj.agg_by_rdtype_funcls_avgspd()
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
        erlt_running_obj.close_conn()
        del erlt_running_obj
