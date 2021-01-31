"""
Module to execute SQL commands for starts emission process.
Created by: Apoorba Bibeka
Created on: 01/29/2021
"""
import time
import pandas as pd
import mariadb
import os
import numpy as np
import datetime
import logging
from ttierlt.movesdb import MovesDb
from ttierlt.utils import connect_to_server_db, get_db_nm_list, PATH_INTERIM_EXTNIDLE

def create_extnidle_table_in_db(delete_if_exists=False):
    pass


# noinspection SpellCheckingInspection
class ExtnidleSqlCmds(MovesDb):
    """
    Class to execute SQL commands for extended idling emission process.
    """
    sourcetypedict={"Combination Long-haul Truck": 62}
    def __init__(self, db_nm_):
        super().__init__(db_nm_=db_nm_)
        self.moves2014b_db_nm = "movesdb20181022"
        self.head_extnidlerate_df = pd.DataFrame()
        self.hourmix_extidle = pd.DataFrame()
        self.created_all_indices = False

    def aggregate_extnidlerate_rateperhour(self, debug=True):
        """
        Script creates the required extended idling rate table from MOVES output databases
        Only required pollutants are selected based on the rateperhour output table
        Emission rates are summed yearID,monthid,hourID, pollutantID, sourceTypeID, fuelTypeID, processid.
        Parameters
        ----------
        debug: bool
            True, to save a sample of the extnidlerate table.
        Returns
        -------
        pd.DataFrame()
            Returns empty pd.DataFrame() when debug = False; return first 5 rows of extnidlerate if debug=True.
        """
        start_time = time.time()
        self.cur.execute("FLUSH TABLES;")
        self.cur.execute("DROP TABLE  IF EXISTS Extnidlerate;")
        self.cur.execute(
            f"""
            CREATE TABLE Extnidlerate
            SELECT yearID, monthid, hourID, pollutantID, sourceTypeID, fuelTypeID, processid,sum(ratePerHour) as rateperhour 
            FROM rateperhour
            WHERE pollutantid in (2,3,31,33,87,98,100,110,20, 23, 185,24,25,26,27,41,68, 69,70,71, 72, 73, 74, 75, 76, 77, 78,
            81, 82, 83, 84, 168, 169, 170, 171, 172, 173, 174, 175, 176, 177,178, 181, 182, 183, 184) and sourceTypeID = self.sourcetypedict["Combination Long-haul Truck"]
            group by yearID,monthid,hourID, pollutantID, sourceTypeID, fuelTypeID, processid;
        """
        )
        self._update_extnidlerate_rateperhour()
        logging.info(
            "---aggregate_extnidlerate_rateperhour and _update_extnidlerate_rateperhour execution time:  %s seconds "
            "---" % (time.time() - start_time)
        )
        if debug:
            self.head_startrate_df = pd.read_sql(
                f"SELECT * FROM startrate LIMIT 5", self.conn
            )
            print(
                "---aggregate_extnidlerate_rateperhour and _update_extnidlerate_rateperhour execution time:  %s seconds "
                "---" % (time.time() - start_time)
            )
            return self.head_extnidlerate_df
        return pd.DataFrame()

        pass

    def _update_extnidlerate_rateperhour(self):
        pass

    def get_hourmix_extidle(self):
        pass

    def test_hourmix_extidle(self):
        pass

    def create_indices_before_joins(self):
        pass

    def compute_factored_extnidlerate(self):
        pass

    def agg_by_vehtyp_fueltyp(self):
        pass
