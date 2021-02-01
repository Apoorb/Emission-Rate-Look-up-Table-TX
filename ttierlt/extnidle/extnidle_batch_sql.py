"""
Module to execute SQL commands for extended idling emission process.
Created by: Apoorba Bibeka
Created on: 01/29/2021
"""
import time
import pandas as pd
import mariadb
import os
import numpy as np
import logging
from ttierlt.movesdb import MovesDb
from ttierlt.utils import connect_to_server_db, get_db_nm_list, PATH_INTERIM_EXTNIDLE


def create_extnidle_table_in_db(delete_if_exists=False):
    """
    Create  mvs2014b_erlt_out.extnidle_erlt_intermediate table for storing output.
    Parameters
    ----------
    delete_if_exists: Delete the existing mvs2014b_erlt_out.extnidle_erlt_intermediate
    table (if it exists).
    """
    # delete_if_exists: Check if we want to delete the previous stored table
    conn = connect_to_server_db(database_nm=None)
    cur = conn.cursor()
    if delete_if_exists:
        cur.execute(
            "DROP TABLE  IF EXISTS mvs2014b_erlt_out.extnidle_erlt_intermediate"
        )
    cur.execute(
        """
        CREATE TABLE mvs2014b_erlt_out.extnidle_erlt_intermediate (
        `Area` CHAR(25) NULL DEFAULT NULL COLLATE 'utf8_unicode_ci',
        `yearid` SMALLINT(5) UNSIGNED NULL DEFAULT NULL,
        `monthid` SMALLINT(5) UNSIGNED NULL DEFAULT NULL,
        `Processtype` CHAR(25) NULL DEFAULT NULL COLLATE 'utf8_unicode_ci',
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
        CONSTRAINT extnidle_erlt_intermediate_pk PRIMARY KEY (Area, yearid, monthid, 
        Processtype)
        )
        COLLATE='utf8_unicode_ci'
        ENGINE=MyISAM;
    """
    )
    conn.close()


# noinspection SpellCheckingInspection
class ExtnidleSqlCmds(MovesDb):
    """
    Class to execute SQL commands for extended idling emission process.
    """

    sourcetypedict = {"Combination Long-haul Truck": 62}

    def __init__(self, db_nm_):
        super().__init__(db_nm_=db_nm_)
        self.head_extnidlerate_df = pd.DataFrame()
        self.hourmix_extidle = pd.DataFrame()
        self.created_all_indices = False

    def aggregate_extnidlerate_rateperhour(self, debug=True):
        """
        Script creates the required extended idling rate table from MOVES output
        databases. Only required pollutants are selected based on the rateperhour
        output table. Emission rates are summed yearID,monthid,hourID, pollutantID,
        sourceTypeID, fuelTypeID, processid.
        Parameters
        ----------
        debug: bool
            True, to save a sample of the extnidlerate table.
        Returns
        -------
        pd.DataFrame()
            Returns empty pd.DataFrame() when debug = False; return first 5 rows of
            extnidlerate if debug=True.
        """
        start_time = time.time()
        self.cur.execute("FLUSH TABLES;")
        self.cur.execute("DROP TABLE  IF EXISTS Extnidlerate;")
        self.cur.execute(
            f"""
            CREATE TABLE Extnidlerate
            SELECT yearID, monthid, hourID, pollutantID, sourceTypeID, fuelTypeID, 
            processid,sum(ratePerHour) as rateperhour 
            FROM rateperhour
            WHERE pollutantid in (2,3,31,33,87,98,100,110,20, 23, 185,24,25,26,27,41,68, 
            69,70,71, 72, 73, 74, 75, 76, 77, 78, 81, 82, 83, 84, 168, 169, 170, 171, 
            172, 173, 174, 175, 176, 177,178, 181, 182, 183, 184) and sourceTypeID 
            = {self.sourcetypedict["Combination Long-haul Truck"]}
            group by yearID,monthid,hourID, pollutantID, sourceTypeID, fuelTypeID, 
            processid;
        """
        )
        self._update_extnidlerate_rateperhour()
        logging.info(
            "---aggregate_extnidlerate_rateperhour and _update_extnidlerate_rateperhour"
            " execution time:  %s seconds "
            "---" % (time.time() - start_time)
        )
        if debug:
            self.head_extnidlerate_df = pd.read_sql(
                f"SELECT * FROM Extnidlerate LIMIT 5", self.conn
            )
            print(
                "---aggregate_extnidlerate_rateperhour and "
                "_update_extnidlerate_rateperhour execution time:  %s seconds "
                "---" % (time.time() - start_time)
            )
            return self.head_extnidlerate_df
        return pd.DataFrame()

    def _update_extnidlerate_rateperhour(self):
        """
        Function to add necessary fields to the rate table and populate it with
        apprropriate data.
        """
        self.cur.execute("FLUSH TABLES;")
        self.cur.execute(
            """
            ALTER TABLE Extnidlerate
            ADD COLUMN Hourmix float,
            ADD COLUMN Area char(25),
            ADD COLUMN Processtype char(25),
            ADD COLUMN txledfac float(6),
            ADD COLUMN emisFact float;
        """
        )
        self.cur.execute("FLUSH TABLES;")
        self.cur.execute("""Update Extnidlerate SET Area = @analysis_district;""")
        self.cur.execute(
            "UPDATE Extnidlerate "
            "SET Processtype = 'Extnd_Exhaust' WHERE Processid in (17,90);"
        )
        self.cur.execute(
            "UPDATE Extnidlerate SET Processtype = 'APU' WHERE Processid = 91;"
        )

    def get_hourmix_extidle(self):
        """
        -- Function creates the hour-mix table from the MOVES default database
        -- Note that default databse need to be present in order to execute
        this  query
        -- If MOVES model version is updated by EPA, MOVES default schema referenced
        here need to be changed.
        """
        self.cur.execute("FLUSH TABLES;")
        self.cur.execute("DROP TABLE  IF EXISTS hourmix_extidle;")
        self.cur.execute(
            f"""
            CREATE TABLE hourmix_extidle
            SELECT a.hourID,b.hotellingdist from {self.moves2014b_db_nm}.hourday a
            JOIN {self.moves2014b_db_nm}.sourcetypehour b on 
            a.hourdayid = b.hourdayid
            where a.dayid = '5';
        """
        )
        self.hourmix_extidle = pd.read_sql(f"SELECT * FROM hourmix_extidle", self.conn)
        self.test_hourmix_extidle()
        return self.hourmix_extidle

    def test_hourmix_extidle(self):
        assert np.allclose(
            self.hourmix_extidle.hotellingdist.sum(),
            1,
        )

    def create_indices_before_joins(self):
        """Create indices for all tables before join to speed-up the join."""
        try:
            self.cur.execute(
                """CREATE INDEX IF NOT EXISTS extnidleidx1 ON Extnidlerate (hourID);"""
            )
            self.cur.execute(
                """
            CREATE INDEX IF NOT EXISTS hrmix_extidx1 ON hourmix_extidle (hourID);"""
            )
            if self.use_txled:
                self.cur.execute(
                    "CREATE INDEX IF NOT EXISTS extnidleidx2 "
                    "ON Extnidlerate (pollutantid, sourcetypeid, fueltypeid);"
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
                "Run aggregate_extnidlerate_rateperhour, get_hourmix_extidle, "
                "get_txled_for_db_district_year functions"
                "before creating indices."
            )
            raise

    def join_extnidlerate_txled_hourmix(self):
        """
        Join hotelling distribution and TxLED emission reduction factors to the
        Extnidlerate table.
        """
        start_time = time.time()
        if self.created_all_indices:
            self.cur.execute("FLUSH TABLES;")
            self.cur.execute(
                """
                UPDATE Extnidlerate a
                JOIN hourmix_extidle b
                ON a.hourID = b.hourID
                SET a.Hourmix = b.hotellingdist;
            """
            )
            if self.use_txled:
                self.cur.execute(
                    f"""
                    UPDATE Extnidlerate a
                    LEFT JOIN txled_long_{self.analysis_year}  d  ON
                    a.pollutantid = d.pollutantid AND
                    a.sourcetypeid = d.sourcetypeid AND
                    a.fueltypeid = d.fueltypeid
                    SET a.txledfac = d.txled_fac;
                """
                )
                self.cur.execute(
                    f"""
                    UPDATE Extnidlerate
                    SET txledfac = 1.0 WHERE txledfac IS NULL;
                """
                )
            else:
                self.cur.execute(f"""UPDATE Extnidlerate SET txledfac = 1.0;""")
        else:
            print(
                "Run create_indices_before_joins to speed-up joins. Will not run this "
                "function unless create_indices_before_joins ran without errors."
            )
            raise ValueError("self.created_all_indices is still False.")
        print(
            "---join_extnidlerate_txled_hourmix execution time:  %s seconds---"
            % (time.time() - start_time)
        )
        logging.info(
            "---join_extnidlerate_txled_hourmix execution time:  %s seconds---"
            % (time.time() - start_time)
        )

    def compute_factored_extnidlerate(self):
        """Weight the emission rate by time of day hotelling distribution and
        TxLED factor for counties where TxLED
        program is active in a county (or majority of county of a district.)"""
        self.cur.execute(
            """UPDATE Extnidlerate SET emisFact = rateperhour*HourMix*txledfac;"""
        )

    def agg_by_processtype(
        self, add_seperate_conflicted_copy=False, conflicted_copy_suffix=""
    ):
        """
        Aggregate (sum) emission rate by yearid, monthid, Processtype. Insert the
        aggregated table to mvs2014b_erlt_out.extnidle_erlt_intermediate if no
        duplicate exists. Alternatively, save a conflicted copy
        mvs2014_erlt_conflicted schema.
        """
        start_time = time.time()
        cmd_insert = """
                INSERT INTO mvs2014b_erlt_out.extnidle_erlt_intermediate( Area, yearid, 
                monthid, Processtype, 
                CO, NOX, SO2, NO2, VOC, CO2EQ, PM10, PM25, BENZ, NAPTH, BUTA, FORM, 
                ACTE, ACROL, ETYB, DPM, POM)
        """
        cmd_create_conflicted = (f"CREATE TABLE mvs2014b_erlt_conflicted.extnidle"
                                 f"_{self.district_abb}_{self.analysis_year}_"
                                 f"{self.anaylsis_month}_{conflicted_copy_suffix}")
        cmd_common = """
            SELECT Area, yearid, monthid, Processtype,
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
            FROM Extnidlerate
            GROUP BY yearid, monthid, Processtype;
        """
        try:
            cmd_insert_agg = cmd_insert + cmd_common
            if not add_seperate_conflicted_copy:
                self.cur.execute(cmd_insert_agg)
            else:
                print(
                    f"Saving starts emission rate for {self.district_abb}, "
                    f"{self.analysis_year}, "
                    f"{self.anaylsis_month} in mvs2014b_erlt_conflicted for review."
                )
                cmd_create_agg = cmd_create_conflicted + cmd_common
                self.cur.execute(
                    f"DROP TABLE IF EXISTS mvs2014b_erlt_conflicted.extnidle"
                    f"_{self.district_abb}_{self.analysis_year}"
                    f"_{self.anaylsis_month}_{conflicted_copy_suffix};"
                )
                self.cur.execute(cmd_create_agg)
            print(
                "---agg_by_processtype execution time:  %s seconds---"
                % (time.time() - start_time)
            )
            logging.info(
                "---agg_by_processtype execution time:  %s seconds---"
                % (time.time() - start_time)
            )
        except mariadb.IntegrityError as integerityrr:
            print(integerityrr)
            print(
                "Re-create the mvs2014b_erlt_out.extnidle_erlt_intermediate "
                "table if you want to overwrite it."
            )
            print(
                f"Cannot write over the data in "
                f"mvs2014b_erlt_out.extnidle_erlt_intermediate. Drop the rows "
                f"you are trying to overwrite"
            )
            raise
        except mariadb.ProgrammingError as programmingerr:
            print(programmingerr)
            raise


if __name__ == "__main__":
    path_to_log_dir = os.path.join(PATH_INTERIM_EXTNIDLE, "Log Files")
    if not os.path.exists(path_to_log_dir):
        os.mkdir(path_to_log_dir)
    path_log_file = os.path.join(path_to_log_dir, "extnidle_test_sql.log")
    logging.basicConfig(filename=path_log_file, filemode="w", level=logging.INFO)
    # ---
    db_nms_list = get_db_nm_list(county_abb="elp")
    db_nm = "mvs14b_erlt_aus_48141_2020_1_cer_out"
    logging.info(f"# Start processing {db_nm}")
    elp_2022_7_obj = ExtnidleSqlCmds(db_nm_=db_nm)
    query_start_time = time.time()
    sample_extnidlerate = elp_2022_7_obj.aggregate_extnidlerate_rateperhour()
    hourmix_extidle = elp_2022_7_obj.get_hourmix_extidle()
    txled_elp_dict = elp_2022_7_obj.get_txled()
    elp_2022_7_obj.create_indices_before_joins()
    elp_2022_7_obj.join_extnidlerate_txled_hourmix()
    elp_2022_7_obj.compute_factored_extnidlerate()
    elp_2022_7_obj.agg_by_processtype(
        add_seperate_conflicted_copy=False, conflicted_copy_suffix="drop_after_testing"
    )
    elp_2022_7_obj.close_conn()
    logging.info(
        "---Query execution time:  %s seconds ---" % (time.time() - query_start_time)
    )
    logging.info(f"# End processing {db_nm}")
    del elp_2022_7_obj
