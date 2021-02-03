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
        cur.execute("DROP TABLE  IF EXISTS mvs2014b_erlt_out.idling_erlt_intermediate")
        # TODO: Add code to create output table
    cur.execute(
        """
        CREATE TABLE mvs2014b_erlt_out.idling_erlt_intermediate (
        `Area` CHAR(25) NULL DEFAULT NULL COLLATE 'utf8_unicode_ci',
        `yearid` SMALLINT(5) UNSIGNED NULL DEFAULT NULL,
        `monthid` SMALLINT(5) UNSIGNED NULL DEFAULT NULL,
        `hourid` SMALLINT(5) UNSIGNED NULL DEFAULT NULL,
        `period` CHAR(2) NULL DEFAULT NULL COLLATE 'utf8_unicode_ci',
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
        CONSTRAINT idling_erlt_intermediate_pk PRIMARY KEY (Area, yearid, monthid, 
        hourid, period)
        )
        COLLATE='utf8_unicode_ci'
        ENGINE=MyISAM; 
        """
    )
    conn.close()


# noinspection SpellCheckingInspection
class IdlingSqlCmds(MovesDb):
    """
    Class to execute SQL commands for idling emission process.
    """

    def __init__(self, db_nm_):
        super().__init__(db_nm_=db_nm_)
        self.moves2014b_db_nm = "movesdb20181022"
        self.head_idlerate_df = pd.DataFrame()
        self.houridlemix = pd.DataFrame()
        self.sutmix = pd.DataFrame()
        self.created_all_indices = False
        self.PROJECT_HOUR_PERIOD_MAP = {"AM": 8, "PM": 18, "MD": 15, "ON": 23}

    def aggregate_idlerate_movesoutput(self, debug=True):
        """
        Script creates the required idlerate table from MOVES output databases.
        Only required pollutants are selected from the movesoutput output table
        Emission are summed overyearid,monthid,hourid,pollutantid,sourcetypeid,
        fueltypeid. Note: Emission are initially not in rates. E.g. NOX is still
        in grams. We will use houridlemix to convert the emission quantity into
        rates.
        Parameters
        ----------
        debug: bool
            True, to save a sample of the idlerate table.
        Returns
        -------
        pd.DataFrame()
            Returns empty pd.DataFrame() when debug = False; return first 5 rows of
            idlerate if debug=True.
        """
        start_time = time.time()
        self.cur.execute("FLUSH TABLES;")
        self.cur.execute(f"DROP TABLE  IF EXISTS idlerate;")
        # FixMe: Make the pollutants a user entered parameter for the class
        self.cur.execute(
            """--
        CREATE TABLE idlerate (SELECT yearid, monthid,hourid,countyid,
        linkid,pollutantid,sourcetypeid,fueltypeid,sum(emissionquant)as emission 
        FROM movesoutput
        GROUP BY yearid,monthid,hourid,countyid,roadtypeid,linkid,pollutantid,
        sourcetypeid,fueltypeid);
        """
        )
        self._update_idlerate_movesoutput()
        logging.info(
            "---aggregate_idlerate_movesoutput and _update_idlerate_movesoutput "
            "execution time:  %s seconds "
            "---" % (time.time() - start_time)
        )
        if debug:
            self.head_idlerate_df = pd.read_sql(
                f"SELECT * FROM idlerate LIMIT 5", self.conn
            )
            print(
                "---aggregate_idlerate_movesoutput and _update_idlerate_movesoutput "
                "execution time:  %s seconds "
                "---" % (time.time() - start_time)
            )
            return self.head_idlerate_df
        return pd.DataFrame()

    def _update_idlerate_movesoutput(self):
        """
        Function to add necessary fields to the rate table and populate it with
        apprropriate data.
        """
        self.cur.execute("FLUSH TABLES;")
        self.cur.execute(
            """
            ALTER TABLE idlerate
            ADD COLUMN stypemix float,
            ADD COLUMN idlerate float,
            ADD COLUMN period char(2),
            ADD COLUMN Area char(25),
            ADD COLUMN VMTmix float,
            ADD COLUMN txledfac float(6),
            ADD COLUMN emisfact float;
        """
        )
        self.cur.execute("UPDATE idlerate SET Area =  @analysis_district;")
        for period_val, hourid in self.PROJECT_HOUR_PERIOD_MAP.items():
            cmd_period_hourid = f"""
                UPDATE idlerate SET period = '{period_val}' 
                WHERE hourid = {hourid};
            """
            self.cur.execute(cmd_period_hourid)

    def get_houridlemix(self):
        """
        Get the fraction of an hour idling for different fuel type for all vehicle
        types. For instance, a gasoline passanger car might be idling for 0.9 hour
        and a diesel passenger car might be idling for 0.1 hour for the a give
        scenario. We will use this information to get the emission rate =
        Total emission / Fraction of the hour idling.
        e.g. Gasoline passanger car emission for 0.9 hour = X grams
        Gasoline passanger car Emission rate = Gasoline passanger car emission
        for 1 hour = X / 0.9 grams / hour
        Note: Activity type id = 4 is source operating hour.
        """
        self.cur.execute("FLUSH TABLES;")
        self.cur.execute(f"DROP TABLE IF EXISTS houridlemix;")
        self.cur.execute(
            """
            CREATE TABLE houridlemix (SELECT yearid,monthid,hourid,
            linkid,sourcetypeid,fueltypeid,sum(activity)as vmx 
            FROM movesactivityoutput
            WHERE activitytypeid = 4 AND activity > 0
            GROUP BY yearid,monthid,hourid,countyid,linkid,sourcetypeid,fueltypeid);
        """
        )
        self.houridlemix = pd.read_sql(f"SELECT * FROM houridlemix", self.conn)
        self.test_houridlemix()
        return self.houridlemix

    def test_houridlemix(self):
        assert np.allclose(
            self.houridlemix.groupby(
                ["yearid", "monthid", "hourid", "linkid", "sourcetypeid"]
            )
            .vmx.sum()
            .values,
            1,
        )

    def get_sutmix(self):
        # TODO: Check why did we filter to  vmx_rdcode = 5 (Urban unrestricted)
        """
        Get the fraction of different vehicle/ source types based on the
        vmtmix_fy20.todmix vmx_rdcode = 5 (Urban unrestricted)
        """
        self.cur.execute("FLUSH TABLES;")
        self.cur.execute(f"DROP TABLE IF EXISTS SUTmix;")
        self.cur.execute(
            """
            CREATE TABLE SUTmix 
            SELECT * FROM vmtmix_fy20.todmix 
            WHERE TxDOT_Dist = @analysis_district  
            AND Daytype = "Weekday" AND YearID = @analysis_year_todmix 
            AND VMX_RDcode = 5;
        """
        )
        self.sutmix = pd.read_sql(f"SELECT * FROM SUTmix", self.conn)
        self.test_sutmix()
        return self.sutmix

    def test_sutmix(self):
        assert np.allclose(
            self.sutmix.groupby(["Period"]).VMTmix.sum().values,
            1,
        )

    def create_indices_before_joins(self):
        """Create indices for all tables before join to speed-up the join."""
        try:
            self.cur.execute(
                """CREATE INDEX IF NOT EXISTS idleidx1
                ON idlerate (monthid, hourid, linkid, sourcetypeid, fueltypeid);"""
            )
            self.cur.execute(
                f"""CREATE INDEX IF NOT EXISTS  houridlemix_idx1 
                ON houridlemix (monthid, hourid, linkid, sourcetypeid, fueltypeid);"""
            )
            self.cur.execute(
                """CREATE INDEX IF NOT EXISTS idleidx2 
                ON idlerate (period, sourcetypeID, fueltypeID);"""
            )
            self.cur.execute(
                """CREATE INDEX IF NOT EXISTS sutmixidx1 
                ON SUTmix (period, MOVES_STcode, MOVES_FTcode);"""
            )
            if self.use_txled:
                self.cur.execute(
                    """CREATE INDEX IF NOT EXISTS  idleidx3 
                    ON idlerate (pollutantid, sourcetypeid, fueltypeid);"""
                )
                self.cur.execute(
                    f"""
                    CREATE INDEX IF NOT EXISTS txledidx1 
                    ON txled_long_{self.analysis_year}
                    (pollutantid, sourcetypeid, fueltypeid);
                """
                )
            self.created_all_indices = True
        except mariadb.ProgrammingError as mdberr:
            print(mdberr)
            print(
                "Run aggregate_idlerate_movesoutput, get_houridlemix, "
                "get_sutmix, get_txled functions before creating indices."
            )
            raise

    def join_idlerate_houridlemix_sutmix_txled(self):
        """
        Join fraction of an hour idling for different fuel type for all vehicle,
        fraction of different vehicle/ source types , and TxLED
        emission reduction factors to the idlerate table.
        """
        start_time = time.time()
        if self.created_all_indices:
            self.cur.execute("FLUSH TABLES;")
            self.cur.execute(
                f"""
                UPDATE idlerate A
                JOIN houridlemix B ON
                (A.monthid = B.monthid AND
                A.hourid = B.hourid AND
                A.linkid = B.linkid AND
                A.sourcetypeid = B.sourcetypeid AND
                A.fueltypeid = B.fueltypeid)
                SET A.stypemix = B.vmx;
            """
            )
            self.cur.execute(
                f"""
                UPDATE idlerate a
                JOIN SUTmix  b ON
                a.period = b.period AND
                a.sourcetypeID = b.MOVES_STcode AND
                a.fueltypeID = b.MOVES_FTcode 
                SET a.VMTmix = b.VMTmix;
            """
            )
            if self.use_txled:
                self.cur.execute(
                    f"""
                    UPDATE idlerate a
                    LEFT JOIN txled_long_{self.analysis_year} d  ON
                    a.pollutantid = d.pollutantid AND
                    a.sourcetypeid = d.sourcetypeid AND
                    a.fueltypeid = d.fueltypeid
                    SET a.txledfac = d.txled_fac;
                """
                )
                self.cur.execute(
                    f"""
                    UPDATE idlerate
                    SET txledfac = 1.0 WHERE txledfac IS NULL;
                """
                )
            else:
                self.cur.execute(f"""UPDATE idlerate SET txledfac = 1.0;""")
        else:
            print(
                "Run create_indices_before_joins to speed-up joins. Will not run this "
                "function unless create_indices_before_joins ran without errors."
            )
            raise ValueError("self.created_all_indices is still False.")
        print(
            "---join_idlerate_houridlemix_sutmix_txled execution time:  %s seconds---"
            % (time.time() - start_time)
        )
        logging.info(
            "---join_idlerate_houridlemix_sutmix_txled execution time:  %s seconds---"
            % (time.time() - start_time)
        )

    def compute_factored_emisrate(self):
        """
        Factor in fraction of an hour idling for different fuel type for all vehicle
        types. For instance, a gasoline passanger car might be idling for 0.9 hour
        and a diesel passenger car might be idling for 0.1 hour for the a give
        scenario. We will use this information to get the emission rate =
        Total emission / Fraction of the hour idling.
        e.g. Gasoline passanger car emission for 0.9 hour = X grams
        Gasoline passanger car Emission rate = Gasoline passanger car emission
        for 1 hour = X / 0.9 grams / hour
        Note: Activity type id = 4 is source operating hour.

        Weight the emission rate by vmt/ vehicle-fuel distribution for different
        vehicle types, fuel types proportion during different time of day and factor
        in if the TxLED program is active in a county (or majority of county of a
        district
        """
        self.cur.execute("UPDATE idlerate SET idlerate = emission / stypemix ;")
        self.cur.execute("UPDATE idlerate SET emisfact = idlerate * VMTmix * txledfac;")

    def agg_by_hourid_period(
        self, add_seperate_conflicted_copy=False, conflicted_copy_suffix=""
    ):
        """
        Aggregate (sum) emission rate by Area, yearid, monthid, hourid, period.
        Insert the aggregated table to mvs2014b_erlt_out.idling_erlt_intermediate if
        no duplicate exists. Else, ask the user if they want a conflicted copy saved
        in mvs2014_erlt_conflicted schema.
        """
        start_time = time.time()
        cmd_insert = """
                INSERT INTO mvs2014b_erlt_out.idling_erlt_intermediate(Area, yearid, 
                monthid,hourid,period,
                CO, NOX, SO2, NO2, VOC, CO2EQ, PM10, PM25, BENZ, NAPTH, BUTA, FORM, 
                ACTE, ACROL, ETYB, DPM, POM)
        """
        cmd_create_conflicted = (
            f"CREATE TABLE mvs2014b_erlt_conflicted.idling"
            f"_{self.district_abb}_{self.analysis_year}_"
            f"{conflicted_copy_suffix}"
        )
        cmd_common = """
                SELECT Area,yearid,monthid,hourid,period,
                SUM(IF(pollutantid = 2, emisfact, 0)) AS CO,
                SUM(IF(pollutantid = 3, emisfact, 0)) AS NOX,
                SUM(IF(pollutantid = 31, emisfact, 0)) AS SO2,
                SUM(IF(pollutantid = 33, emisfact, 0)) AS NO2,
                SUM(IF(pollutantid = 87, emisfact, 0)) AS VOC,
                SUM(IF(pollutantid = 98, emisfact, 0)) AS CO2EQ,
                SUM(IF(pollutantid = 100, emisfact, 0)) AS PM10,
                SUM(IF(pollutantid = 110, emisfact, 0)) AS PM25,
                SUM(IF(pollutantid = 20, emisfact, 0)) AS BENZ,
                SUM(IF(pollutantid IN (23, 185), emisfact, 0)) AS NAPTH,
                SUM(IF(pollutantid = 24, emisfact, 0)) AS BUTA,
                SUM(IF(pollutantid = 25, emisfact, 0)) AS FORM,
                SUM(IF(pollutantid = 26, emisfact, 0)) AS ACTE,
                SUM(IF(pollutantid = 27, emisfact, 0)) AS ACROL,
                SUM(IF(pollutantid = 41, emisfact, 0)) AS ETYB,
                SUM(IF(pollutantid = 100 AND fueltypeid = 2, emisfact, 0)) AS DPM,
                SUM(IF(pollutantid IN (68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78,
                81, 82, 83, 84, 168, 169, 170, 171, 172, 173, 174, 175, 176, 177,
                178, 181, 182, 183, 184), emisfact, 0)) AS POM
                FROM idlerate
                GROUP BY Area,yearid,monthid,hourid,period;
        """
        try:
            cmd_insert_agg = cmd_insert + cmd_common
            if not add_seperate_conflicted_copy:
                self.cur.execute(cmd_insert_agg)
            else:
                print(
                    f"Saving idling emission rate for "
                    f"{self.district_abb}, {self.analysis_year}, "
                    f" in mvs2014b_erlt_conflicted for review."
                )
                cmd_create_agg = cmd_create_conflicted + cmd_common
                self.cur.execute(
                    f"DROP TABLE IF EXISTS mvs2014b_erlt_conflicted.idling"
                    f"_{self.district_abb}_{self.analysis_year}"
                    f"_{conflicted_copy_suffix};"
                )
                self.cur.execute(cmd_create_agg)
            print(
                "---agg_by_hourid_period execution time:  %s seconds---"
                % (time.time() - start_time)
            )
            logging.info(
                "---agg_by_hourid_period execution time:  %s seconds---"
                % (time.time() - start_time)
            )
        except mariadb.IntegrityError as integerityrr:
            print(integerityrr)
            print(
                "Re-create the mvs2014b_erlt_out.idling_erlt_intermediate table if you"
                " want to overwrite it."
            )
            print(
                f"Cannot write over the data in "
                f"mvs2014b_erlt_out.idling_erlt_intermediate. Drop the rows you are"
                f"trying to overwrite"
            )
            raise


if __name__ == "__main__":
    path_to_log_dir = os.path.join(PATH_INTERIM_IDLING, "Log Files")
    if not os.path.exists(path_to_log_dir):
        os.mkdir(path_to_log_dir)
    path_log_file = os.path.join(path_to_log_dir, "running_test_sql.log")
    logging.basicConfig(filename=path_log_file, filemode="w", level=logging.INFO)
    # ---
    db_nms_list = get_db_nm_list(district_abb="elp")
    db_nm = "mvs14b_erlt_elp_48141_2022_per_out"
    logging.info(f"# Start processing {db_nm}")
    db_sql_obj = IdlingSqlCmds(db_nm_=db_nm)
    query_start_time = time.time()
    head_idlerate_df = db_sql_obj.aggregate_idlerate_movesoutput()
    hourmix = db_sql_obj.get_houridlemix()
    sutmix = db_sql_obj.get_sutmix()
    txled_elp_dict = db_sql_obj.get_txled()
    db_sql_obj.create_indices_before_joins()
    db_sql_obj.join_idlerate_houridlemix_sutmix_txled()
    db_sql_obj.compute_factored_emisrate()
    db_sql_obj.agg_by_hourid_period(
        add_seperate_conflicted_copy=False, conflicted_copy_suffix="drop_after_testing"
    )
    db_sql_obj.close_conn()
    logging.info(
        "---Query execution time:  %s seconds ---" % (time.time() - query_start_time)
    )
    logging.info(f"# End processing {db_nm}")
    del db_sql_obj
