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
from ttierlt.utils import PATH_INTERIM, get_db_nm_list
from ttierlt.utils import create_qaqc_output_conflicted_schema
from ttierlt.running.batch_sql import create_running_table_in_db
from ttierlt.running.batch_sql import RunningSqlCmds as erlt_running

DEBUG = True

if __name__ == "__main__":
    path_to_log_dir = os.path.join(PATH_INTERIM, "Log Files")
    if not os.path.exists(path_to_log_dir):
        os.mkdir(path_to_log_dir)
    logfilenm = datetime.datetime.now().strftime('log_02_batch_run_el_passo_running_files_%H_%M_%d_%m_%Y.log')
    path_log_file = os.path.join(path_to_log_dir, logfilenm)
    logging.basicConfig(filename=path_log_file, filemode='w', level=logging.INFO)
    db_nms_list = get_db_nm_list(county_abb="elp")
    list_erlt_dfs = []
    STOP_ITER = 2
    ITER_CNTER = 0
    for db_nm in db_nms_list:
        start_time = time.time()
        if DEBUG & ITER_CNTER == STOP_ITER:
            break
        ITER_CNTER = ITER_CNTER + 1
        create_qaqc_output_conflicted_schema()
        # Delete the existing output table. It cannot have duplicated data; will raise error if you try to add
        # duplicated data.
        create_running_table_in_db(delete_if_exists=True)
        # Run the SQL Commands on the database.
        ########################################################
        logging.info(f"# Start processing {db_nm}")
        print("# Start processing {db_nm}")
        print("-------------------------------------------------------------------")
        erlt_running_obj = erlt_running(
            db_nm_=db_nm,
            county_abb_="elp"
        )
        query_start_time = time.time()
        erlt_running_obj.aggregate_emisrate_rateperdist()
        hourmix_elp = erlt_running_obj.get_hour_mix_for_db_district()
        vmt_mix_elp_2022 = erlt_running_obj.get_vmt_mix_for_db_district_weekday_closest_vmt_yr()
        txled_elp_dict = erlt_running_obj.get_txled_for_db_district_year()
        erlt_running_obj.create_indices_before_joins()
        erlt_running_obj.join_emisrate_vmt_tod_txled()
        erlt_running_obj.compute_factored_emisrate()
        erlt_running_obj.agg_by_rdtype_funcls_avgspd()
        logging.info("---Query execution time:  %s seconds ---" % (time.time() - query_start_time))
        logging.info(f"# End processing {db_nm}")
        print("---Query execution time:  %s seconds ---" % (time.time() - query_start_time))
        print(f"# End processing {db_nm}")
        print("-------------------------------------------------------------------")