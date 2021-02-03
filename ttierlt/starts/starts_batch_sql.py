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
import logging
from ttierlt.movesdb import MovesDb
from ttierlt.utils import connect_to_server_db, get_db_nm_list, PATH_INTERIM_STARTS


def create_starts_table_in_db(delete_if_exists=False):
    """
    Create  mvs2014b_erlt_out.starts_erlt_intermediate table for storing output.
    Parameters
    ----------
    delete_if_exists: Delete the existing mvs2014b_erlt_out.starts_erlt_intermediate table (if it exists).
    """
    # delete_if_exists: Check if we want to delete the previous stored table
    conn = connect_to_server_db(database_nm=None)
    cur = conn.cursor()
    if delete_if_exists:
        cur.execute("DROP TABLE  IF EXISTS mvs2014b_erlt_out.starts_erlt_intermediate")
    cur.execute(
        """
            CREATE TABLE mvs2014b_erlt_out.starts_erlt_intermediate (
            `Area` CHAR(25) NULL DEFAULT NULL COLLATE 'utf8_unicode_ci',
            `yearid` SMALLINT(5) UNSIGNED NULL DEFAULT NULL,
            `monthid` SMALLINT(5) UNSIGNED NULL DEFAULT NULL,
            `VehicleType` CHAR(50) NULL DEFAULT NULL COLLATE 'utf8_unicode_ci',
            `FUELTYPE` CHAR(10) NULL DEFAULT NULL COLLATE 'utf8_unicode_ci',
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
            CONSTRAINT starts_erlt_intermediate_pk PRIMARY KEY (Area, yearid, monthid, 
            vehicletype, fueltype)
        )
        COLLATE='utf8_unicode_ci'
        ENGINE=MyISAM;
    """
    )
    conn.close()


class StartSqlCmds(MovesDb):
    """
    Class to execute SQL commands for starts emission process.
    """

    def __init__(self, db_nm_):
        super().__init__(db_nm_=db_nm_)
        self.fueltypedict = {1: "Gasoline", 2: "Diesel"}
        self.head_startrate_df = pd.DataFrame()
        self.hourmix_starts = pd.DataFrame()
        self.created_all_indices = False

    def aggregate_startrate_rateperstart(self, debug=True):
        """
        Script creates the required start rate table from MOVES output databases
        Only required pollutants are selected based on the rateperstart output table
        Emission rates are summed overyearid,monthid,hourid,pollutantid,sourcetypeid,
        fueltypeid.
        Parameters
        ----------
        debug: bool
            True, to save a sample of the startrate table.
        Returns
        -------
        pd.DataFrame()
            Returns empty pd.DataFrame() when debug = False; return first 5 rows of
            startrate if debug=True.
        """
        start_time = time.time()
        self.cur.execute("FLUSH TABLES;")
        self.cur.execute("DROP TABLE  IF EXISTS startrate;")
        try:
        # FixMe: Make the pollutants a user entered parameter for the class
            self.cur.execute(
                """
                CREATE TABLE startrate (SELECT yearid, monthid,hourid,
                pollutantid,sourcetypeid,fueltypeid,sum(rateperstart)as ERate 
                FROM rateperstart
                WHERE pollutantid in (2,3,31,33,87,98,100,110,20, 23, 185,24,25,26,27,
                41,68, 69,70,71, 72, 73, 74, 75, 76, 77, 78, 81, 82, 83, 84, 168, 169, 
                170, 171, 172, 173, 174, 175, 176, 177,178, 181, 182, 183, 184)
                GROUP BY yearid,monthid,hourid,pollutantid,sourcetypeid,fueltypeid);
            """
            )
        except mariadb.InternalError as intererr:
            print(intererr)
            print(f"Try to recopy the {self.db_nm} from the shared drive.")
            print(f"Dropping the corrupted database: {self.db_nm} ")
            logging.debug(
                f"Try to recopy the {self.db_nm} from the shared drive. "
                f"Dropping the corrupted database: {self.db_nm}"
            )
            self.cur.execute(f"DROP DATABASE {self.db_nm}")
            self.close_conn()
            raise
        except mariadb.OperationalError as operr:
            print(operr)
            print(f"Try to recopy the {self.db_nm} from the shared drive.")
            print(f"Dropping the corrupted database: {self.db_nm} ")
            logging.debug(
                f"Try to recopy the {self.db_nm} from the shared drive. "
                f"Dropping the corrupted database: {self.db_nm}"
            )
            self.cur.execute(f"DROP DATABASE {self.db_nm}")
            self.close_conn()
            raise
        self._update_startrate_rateperstart()
        self._update_sourcetypename_joins()
        logging.info(
            "---aggregate_startrate_rateperstart and _update_startrate_rateperstart "
            "execution time:  %s seconds "
            "---" % (time.time() - start_time)
        )
        if debug:
            self.head_startrate_df = pd.read_sql(
                f"SELECT * FROM startrate LIMIT 5", self.conn
            )
            print(
                "---aggregate_startrate_rateperstart and _update_startrate_rateperstart"
                " execution time:  %s seconds "
                "---" % (time.time() - start_time)
            )
            return self.head_startrate_df
        return pd.DataFrame()

    def _update_startrate_rateperstart(self):
        """
        # Script to add necessary fields to the startrate table.
        """
        cmd_add_additional_fields = """
            FLUSH TABLES;
            ALTER TABLE startrate
            ADD COLUMN Hourmix float,
            ADD COLUMN emisFact float,
            ADD COLUMN Area char(25),
            ADD COLUMN VehicleType char(50),
            ADD COLUMN FuelType char(10),
            ADD COLUMN txledfac FLOAT(6);
            -- End Code with comment.
        """
        cmd_set_area = """
            FLUSH TABLES;
            UPDATE startrate SET Area =  @analysis_district;
            -- End Code with comment.
        """
        for cmd in cmd_add_additional_fields.split(";") + cmd_set_area.split(";"):
            self.cur.execute(cmd)
        for fueltypeid, fueltypenm in self.fueltypedict.items():
            self.cur.execute("FLUSH TABLES;")
            cmd_set_fueldesc = f"""
                UPDATE startrate 
                SET FuelType = "{fueltypenm}" Where FuelTypeID = {fueltypeid};
            """
            self.cur.execute(cmd_set_fueldesc)

    def _update_sourcetypename_joins(self):
        """Add vehicle type description from the MOVES 2014b database
        (movesdb20181022)."""
        self.cur.execute("FLUSH TABLES;")
        cmd_set_sourcetypenm = f"""
            -- Script to add vehicle type group to each MOVES sourcetypeid
            UPDATE startrate a
            JOIN {self.moves2014b_db_nm}.sourceusetype  b  ON
            a.sourceTypeID = b.sourceTypeID
            SET a.VehicleType = b.sourceTypeName;
        """
        self.cur.execute(cmd_set_sourcetypenm)

    def get_hourmix_starts(self):
        """
        Script creates the start-mix table from the movesactivity output table available
        in the MOVES output supplied for rate development.
        Returns
        -------
        """
        self.cur.execute("FLUSH TABLES;")
        self.cur.execute("DROP TABLE  IF EXISTS hourmix_starts;")
        cmd_hourmix_starts = """
            CREATE TABLE hourmix_starts
            SELECT hourID,sourceTypeID,fuelTypeID, startsPerVehicle 
            FROM startspervehicle;
        """
        self.cur.execute(cmd_hourmix_starts)
        self._update_hourmix_starts()
        self.hourmix_starts = pd.read_sql(f"SELECT * FROM hourmix_starts", self.conn)
        self.test_hourmix_starts()
        return self.hourmix_starts

    def test_hourmix_starts(self):
        assert np.allclose(
            self.hourmix_starts.groupby(["sourceTypeID", "fuelTypeID"])
            .hrmix.sum()
            .values,
            1,
        )

    def _update_hourmix_starts(self):
        """Add additional fields to hourmix_starts and add a daily daily (24 hours)
        starts per vehicle column to hourmix data. This column in then use to get the
        proportion of starts in an hour a fraction of daily starts.
        """
        cmd_add_cols_hourmix_starts = """
        ALTER TABLE hourmix_starts
        ADD COLUMN sumact float,
        ADD COLUMN hrmix float;
        """
        self.cur.execute(cmd_add_cols_hourmix_starts)
        self.cur.execute("FLUSH TABLES;")
        cmd_add_col_daily_starts_by_veh_fueltype = """
            UPDATE hourmix_starts as r
            JOIN 
            (SELECT sourceTypeID, fuelTypeID, SUM(startsPerVehicle) as sumact
            FROM hourmix_starts a 
            GROUP BY sourceTypeID, fuelTypeID) AS grp
            ON                   
            r.sourceTypeID = grp.sourceTypeID  AND
            r.fuelTypeID = grp.fuelTypeID
            SET r.sumact = grp.sumact;
        """
        self.cur.execute(cmd_add_col_daily_starts_by_veh_fueltype)
        self.cur.execute("FLUSH TABLES;")
        cmd_get_prop_starts_in_hour_wrt_daily_starts = """
            UPDATE hourmix_starts
            SET hrmix = startsPerVehicle / sumact;"""
        self.cur.execute(cmd_get_prop_starts_in_hour_wrt_daily_starts)

    def create_indices_before_joins(self):
        """Create indices for all tables before join to speed-up the join."""
        try:
            self.cur.execute(
                """CREATE INDEX IF NOT EXISTS stridx1 
                ON startrate (hourID, sourcetypeid, fueltypeid);"""
            )
            self.cur.execute(
                """
            CREATE INDEX IF NOT EXISTS hrmix_stdx1 
            ON hourmix_starts (hourID, sourcetypeid, fueltypeid);"""
            )
            if self.use_txled:
                self.cur.execute(
                    """CREATE INDEX IF NOT EXISTS stridx2 
                    ON startrate (pollutantid, sourcetypeid, fueltypeid);"""
                )
                self.cur.execute(
                    f"""
                    CREATE INDEX IF NOT EXISTS  txledidx1 
                    ON txled_long_{self.analysis_year} 
                    (pollutantid, sourcetypeid, fueltypeid);
                """
                )
            self.created_all_indices = True
        except mariadb.ProgrammingError as mdberr:
            print(mdberr)
            print(
                "Run aggregate_startrate_rateperstart, get_hourmix_starts, "
                "get_txled_for_db_district_year functions "
                "before creating indices."
            )
            raise

    def join_startrate_txled_hourmix(self):
        """
        Join vehicle starts distribution and TxLED emission reduction factors to the
        startrate table.
        """
        start_time = time.time()
        if self.created_all_indices:
            self.cur.execute("FLUSH TABLES;")
            self.cur.execute(
                """
                UPDATE startrate a
                JOIN hourmix_starts  b  ON
                a.hourID = b.hourID and
                a.sourcetypeID = b.sourcetypeID and
                a.fueltypeID = b.fueltypeID 
                SET a.Hourmix = b.hrmix;	 
            """
            )
            if self.use_txled:
                self.cur.execute(
                    f"""
                    UPDATE startrate a
                    LEFT JOIN txled_long_{self.analysis_year}  d  ON
                    a.pollutantid = d.pollutantid AND
                    a.sourcetypeid = d.sourcetypeid AND
                    a.fueltypeid = d.fueltypeid
                    SET a.txledfac = d.txled_fac;
                """
                )
                self.cur.execute(
                    f"""
                    UPDATE startrate
                    SET txledfac = 1.0 WHERE txledfac IS NULL;
                """
                )
            else:
                self.cur.execute(f"""UPDATE startrate SET txledfac = 1.0;""")
        else:
            print(
                "Run create_indices_before_joins to speed-up joins. Will not run this "
                "function unless create_indices_before_joins ran without errors."
            )
            raise ValueError("self.created_all_indices is still False.")
        print(
            "---join_startrate_txled_hourmix execution time:  %s seconds---"
            % (time.time() - start_time)
        )
        logging.info(
            "---join_startrate_txled_hourmix execution time:  %s seconds---"
            % (time.time() - start_time)
        )

    def compute_factored_startrate(self):
        """Weight the emission rate by time of day starts distribution and TxLED factor
        for counties where TxLED program is active in a county (or majority of county of
        a district.)"""
        self.cur.execute("""UPDATE startrate SET emisFact = ERate*HourMix*txledfac;""")

    def agg_by_vehtyp_fueltyp(
        self, add_seperate_conflicted_copy=False, conflicted_copy_suffix=""
    ):
        """
        Aggregate (sum) emission rate by Area, yearid, monthid, VehicleType, FUELTYP.
        Insert the aggregated table to mvs2014b_erlt_out.starts_erlt_intermediate if
        no duplicate exists. Alternatively, save a conflicted copy
        mvs2014_erlt_conflicted schema.
        """
        start_time = time.time()
        cmd_insert = """
                INSERT INTO mvs2014b_erlt_out.starts_erlt_intermediate( Area, yearid, 
                monthid, VehicleType, FUELTYPE, 
                CO, NOX, SO2, NO2, VOC, CO2EQ, PM10, PM25, BENZ, NAPTH, BUTA, FORM, 
                ACTE, ACROL, ETYB, DPM, POM)
        """
        cmd_create_conflicted = (
            f"CREATE TABLE mvs2014b_erlt_conflicted.starts"
            f"_{self.district_abb}_{self.analysis_year}_"
            f"{self.anaylsis_month}_{conflicted_copy_suffix}"
        )
        cmd_common = """
            SELECT Area,yearid,monthid,VehicleType,FUELTYPE,
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
            FROM startrate
            GROUP BY Area,yearid,monthid,VehicleType,FUELTYPE;
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
                    f"DROP TABLE IF EXISTS mvs2014b_erlt_conflicted.starts"
                    f"_{self.district_abb}_{self.analysis_year}"
                    f"_{self.anaylsis_month}_{conflicted_copy_suffix};"
                )
                self.cur.execute(cmd_create_agg)
            print(
                "---agg_by_vehtyp_fueltyp execution time:  %s seconds---"
                % (time.time() - start_time)
            )
            logging.info(
                "---agg_by_vehtyp_fueltyp execution time:  %s seconds---"
                % (time.time() - start_time)
            )
        except mariadb.IntegrityError as integerityrr:
            print(integerityrr)
            print(
                "Re-create the mvs2014b_erlt_out.starts_erlt_intermediate table if you "
                "want to overwrite it."
            )
            print(
                f"Cannot write over the data in "
                f"mvs2014b_erlt_out.starts_erlt_intermediate. Drop the rows you are"
                f"trying to overwrite"
            )
            raise


if __name__ == "__main__":
    path_to_log_dir = os.path.join(PATH_INTERIM_STARTS, "Log Files")
    if not os.path.exists(path_to_log_dir):
        os.mkdir(path_to_log_dir)
    path_log_file = os.path.join(path_to_log_dir, "starts_test_sql.log")
    logging.basicConfig(filename=path_log_file, filemode="w", level=logging.INFO)
    # ---
    db_nms_list = get_db_nm_list(district_abb="elp")
    db_nm = "mvs14b_erlt_elp_48141_2022_7_cer_out"
    logging.info(f"# Start processing {db_nm}")
    elp_2022_7_obj = StartSqlCmds(db_nm_=db_nm)
    query_start_time = time.time()
    sample_startrate = elp_2022_7_obj.aggregate_startrate_rateperstart()
    hourmix_starts = elp_2022_7_obj.get_hourmix_starts()
    TESTING_TXLED = True
    if TESTING_TXLED:
        elp_2022_7_obj.use_txled = True
    txled_elp_dict = elp_2022_7_obj.get_txled()
    elp_2022_7_obj.create_indices_before_joins()
    elp_2022_7_obj.join_startrate_txled_hourmix()
    elp_2022_7_obj.compute_factored_startrate()
    elp_2022_7_obj.agg_by_vehtyp_fueltyp(
        add_seperate_conflicted_copy=True, conflicted_copy_suffix="drop_after_testing"
    )
    elp_2022_7_obj.close_conn()
    logging.info(
        "---Query execution time:  %s seconds ---" % (time.time() - query_start_time)
    )
    logging.info(f"# End processing {db_nm}")
    del elp_2022_7_obj
