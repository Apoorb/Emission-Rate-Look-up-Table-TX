"""
Module to execute SQL commands for idling emission process.
Created by: Apoorba Bibeka
Created on: 01/31/2021
"""
import time
import pandas as pd
import mariadb
import os
import numpy as np
import datetime
import logging
from ttierlt.movesdb import MovesDb
from ttierlt.utils import connect_to_server_db, get_db_nm_list, PATH_INTERIM_IDLING


def create_idling_table_in_db(delete_if_exists=False):
    """
    Create  mvs2014b_erlt_out.idling_erlt_intermediate table for storing output.
    Parameters
    ----------
    delete_if_exists: Delete the existing mvs2014b_erlt_out.idling_erlt_intermediate
    table (if it exists).
    """
    # delete_if_exists: Check if we want to delete the previous stored table
    conn = connect_to_server_db(database_nm=None)
    cur = conn.cursor()
    if delete_if_exists:
        cur.execute(
            "DROP TABLE  IF EXISTS mvs2014b_erlt_out.idling_erlt_intermediate"
        )
        # TODO: Add code to create output table
    conn.close()


# noinspection SpellCheckingInspection
class ExtnidleSqlCmds(MovesDb):
    """
    Class to execute SQL commands for idling emission process.
    """
    def __init__(self, db_nm_):
        super().__init__(db_nm_=db_nm_)
        self.moves2014b_db_nm = "movesdb20181022"
        self.idlerate = pd.DataFrame()
        self.hoursidlemix = pd.DataFrame()
        self.sutmix = pd.DataFrame()
        self.created_all_indices = False

    def aggregate_idlerate_movesoutput(self):
        pass

    def update_idlerate_movesoutput(self):
        pass
