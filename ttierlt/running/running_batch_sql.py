"""
Module to execute SQL commands for running emission process.
Created by: Apoorba Bibeka
Created on: 01/26/2021
"""
import time
import pandas as pd
import mariadb
import os
import logging
from ttierlt.utils import connect_to_server_db, get_db_nm_list, PATH_INTERIM_RUNNING
from ttierlt.movesdb import MovesDb


def create_running_table_in_db(delete_if_exists=False):
    """
    Create  mvs2014b_erlt_out.running_erlt_intermediate table for storing output.
    Parameters
    ----------
    delete_if_exists: Delete the existing mvs2014b_erlt_out.running_erlt_intermediate table (if it exists).
    """
    # delete_if_exists: Check if we want to delete the previous stored table
    conn = connect_to_server_db(database_nm=None)
    cur = conn.cursor()
    if delete_if_exists:
        cur.execute("DROP TABLE  IF EXISTS mvs2014b_erlt_out.running_erlt_intermediate")
    cur.execute(
        """
        CREATE TABLE mvs2014b_erlt_out.running_erlt_intermediate (
            `Area` CHAR(25) NULL DEFAULT NULL COLLATE 'utf8_unicode_ci',
            `yearid` SMALLINT(5) UNSIGNED NULL DEFAULT NULL,
            `monthid` SMALLINT(5) UNSIGNED NULL DEFAULT NULL,
            `funclass` CHAR(25) NULL DEFAULT NULL COLLATE 'utf8_unicode_ci',
            `avgspeed` FLOAT(3,1) NULL DEFAULT NULL,
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
            CONSTRAINT running_erlt_intermediate_pk PRIMARY KEY (Area, yearid, monthid, funclass, avgspeed)
        )
        COLLATE='utf8_unicode_ci'
        ENGINE=MyISAM;
    """
    )
    conn.close()


class RunningSqlCmds(MovesDb):
    """
    Class to execute SQL commands for running emission process.
    """

    def __init__(self, db_nm_):
        super().__init__(db_nm_=db_nm_)
        self.head_emisrate_df = pd.DataFrame()
        self.hourmix = pd.DataFrame()
        self.vmtmix = pd.DataFrame()
        self.created_all_indices = False

    def aggregate_emisrate_rateperdist(self, debug=True):
        """
        Script creates the required base rate table from MOVES output databases
        Only required pollutants are selected based on the emissionrate output table
        Emission rates are summed over different processes under running emission category.
        Parameters
        ----------
        debug: bool
            True, to save a sample of the emisrate table.
        Returns
        -------
        pd.DataFrame()
            Returns empty pd.DataFrame() when debug = False; return first 5 rows of emisrate if debug=True.
        """
        start_time = time.time()
        self.cur.execute("FLUSH TABLES;")
        self.cur.execute(f"DROP TABLE  IF EXISTS emisrate;")
        self.cur.execute(f"DROP TABLE  IF EXISTS {self.db_nm}.emisrate;")
        self.cur.execute(
            f"""
            CREATE TABLE emisrate (SELECT yearid,monthid,hourid,
            roadtypeid,pollutantid,sourcetypeid,fueltypeid,avgSpeedBinID,
            SUM(rateperdistance) as ERate 
            FROM rateperdistance
            WHERE pollutantid in (2,3,31,33,87,98,100,110,20, 23, 185,24,25,26,27,41,68, 69,70,71, 72, 73, 74, 75, 76, 77, 78,
            81, 82, 83, 84, 168, 169, 170, 171, 172, 173, 174, 175, 176, 177,178, 181, 182, 183, 184) and processid not in (18,19)
            GROUP BY yearid,monthid,hourid,roadtypeid,pollutantid,sourcetypeid,fueltypeid,avgSpeedBinID);
            """
        )
        self._update_emisrate_rateperdist()
        logging.info(
            "---aggregate_emisrate_rateperdist and _update_emisrate_rateperdist execution time:  %s seconds "
            "---" % (time.time() - start_time)
        )
        if debug:
            self.head_emisrate_df = pd.read_sql(
                f"SELECT * FROM emisrate LIMIT 5", self.conn
            )
            print(
                "---aggregate_emisrate_rateperdist and _update_emisrate_rateperdist execution time:  %s seconds "
                "---" % (time.time() - start_time)
            )
            return self.head_emisrate_df
        return pd.DataFrame()

    def _update_emisrate_rateperdist(self):
        # -- Script to add necessary fields to the rate table and populate it with appropriate data
        alter_table_avg_spd_cmds = """FLUSH TABLES;
            ALTER TABLE emisrate
            ADD COLUMN avgspeed float(3,1),
            ADD COLUMN Hourmix float,
            ADD COLUMN stypemix float,
            ADD COLUMN emisFact float,
            ADD COLUMN Funclass char(25),
            ADD COLUMN Period char(2),
            ADD COLUMN Area char(25),
            ADD COLUMN txledfac FLOAT(6);
            -- Empty line
            FLUSH TABLES;
            Update emisrate
            SET Area = @analysis_district;
            -- Script to add speeds to each MOVES avgspeedbinid
            FLUSH TABLES;
            UPDATE emisrate
            SET avgspeed = (avgSpeedBinID - 1) * 5;
            FLUSH TABLES;
            UPDATE emisrate
            SET avgspeed = 2.5
            WHERE avgSpeedBinID = 1;   
            FLUSH TABLES;
            -- End Code with comment.
        """

        for cmds in alter_table_avg_spd_cmds.split(";"):
            self.cur.execute(cmds)
        # Add roadtype description to each MOVES roadtypeid
        for rdtype, rdtypedesc in self.MAP_RD_TYPE.items():
            cmd_rd_type = f"""
                UPDATE emisrate SET Funclass = '{rdtypedesc}' WHERE roadtypeid = {rdtype};
            """
            self.cur.execute(cmd_rd_type)
        # Add period description to each MOVES hourid
        for period_val, hourid_tuple in self.MAP_PERIOD_HOURID.items():
            cmd_period_hourid = f"""
                UPDATE emisrate SET period = '{period_val}' WHERE hourid in {hourid_tuple};
            """
            self.cur.execute(cmd_period_hourid)

    def get_hourmix_for_db_district(self):
        """
        Script creates the hour-mix table from the MOVES database.table vmtmix_fy20.todmix.
        Parameters
        ----------
        Returns
        -------
        pd.DataFrame()
            Returns entire hourmix table.
        """
        self.cur.execute("FLUSH TABLES;")
        self.cur.execute(f"DROP TABLE IF EXISTS hourmix_running_{self.district_abb};")
        self.cur.execute(
            f"""
            CREATE TABLE hourmix_running_{self.district_abb}
            SELECT * FROM vmtmix_fy20.hourmix
            WHERE District = @analysis_district;
            """
        )
        self.hourmix = pd.read_sql(
            f"SELECT * FROM hourmix_running_{self.district_abb}", self.conn
        )
        self.test_hourmix_df_is_read()
        return self.hourmix

    def test_hourmix_df_is_read(self):
        """Test if data was read from the hourmix table."""
        assert len(self.hourmix) >= 1, (
            "No data in hourmix table. Check area_district variable in python and "
            "@analysis_district variable in sql. See if these variable value are present in "
            "District column of vmtmix_fy20.hourmix"
        )

    def get_vmtmix_for_db_district_weekday_closest_vmt_yr(self):
        """
        Script creates the VMT-mix table from the movesactivity output table available in the MOVES output databse
        used for rate development. The input table is stored as vmtmix_fy20.todmix.
        Note:The years are in increments of 5: 2020, 2025, 2030... in vmtmix_fy20.todmix. Use analysis_year_todmix to
        refrence the correct year in vmtmix_fy20.todmix for the analysis_year of this database. E.g. if the analysis
        year of the database is 2022 then the year in vmtmix_fy20.todmix table is 2020.
        Returns
        -------
        pd.DataFrame()
            Returns entire vmtmix table.
        """
        self.cur.execute("FLUSH TABLES;")
        self.cur.execute(
            f"DROP TABLE  IF EXISTS vmtmix_weekday_{self.district_abb}_{self.analysis_year_todmix};"
        )
        self.cur.execute(
            f"""
            CREATE TABLE vmtmix_weekday_{self.district_abb}_{self.analysis_year_todmix} 
            SELECT * FROM vmtmix_fy20.todmix 
            WHERE TxDOT_Dist = @analysis_district and Daytype = "Weekday" and YearID =  @analysis_year_todmix;
            """
        )
        self.vmtmix = pd.read_sql(
            f"SELECT * FROM  vmtmix_weekday_{self.district_abb}_{self.analysis_year_todmix} ",
            self.conn,
        )
        self.test_todmix_df_is_read()
        return self.vmtmix

    def test_todmix_df_is_read(self):
        """Test if data in vmtmix table."""
        assert len(self.vmtmix) >= 1, (
            "No data in hourmix table. Check area_district variable in python and "
            "@analysis_district variable in sql. See if these variable value are present in "
            "District column of vmtmix_fy20.hourmix"
        )

    def create_indices_before_joins(self):
        """Create indices for all tables before join to speed-up the join."""
        try:
            self.cur.execute(
                """CREATE INDEX IF NOT EXISTS efidx1 ON emisrate (Period, sourcetypeid, fueltypeid, roadtypeid);"""
            )
            self.cur.execute(
                """CREATE INDEX IF NOT EXISTS  efidx2 ON emisrate (hourid);"""
            )
            self.cur.execute(
                f"""CREATE INDEX IF NOT EXISTS  houridx1 ON hourmix_running_{self.district_abb} (TOD);"""
            )
            self.cur.execute(
                f"""CREATE INDEX IF NOT EXISTS  vmtidx1 ON vmtmix_weekday_{self.district_abb}_{self.analysis_year_todmix}
                (Period, MOVES_STcode, MOVES_FTcode, VMX_RDcode);"""
            )
            if self.use_txled:
                self.cur.execute(
                    """CREATE INDEX IF NOT EXISTS  efidx3 ON emisrate (pollutantid, sourcetypeid, fueltypeid);"""
                )
                self.cur.execute(
                    f"""
                    CREATE INDEX IF NOT EXISTS  txledidx1 ON txled_long_{self.analysis_year}
                    (pollutantid, sourcetypeid, fueltypeid);
                """
                )
            self.created_all_indices = True
        except mariadb.ProgrammingError as mdberr:
            print(mdberr)
            print(
                "Run aggregate_emisrate_rateperdist, get_hour_mix_for_db_district, "
                "get_vmt_mix_for_db_district_weekday_closest_vmt_yr, get_txled_for_db_district_year functions "
                "before creating indices."
            )
            raise

    def join_emisrate_vmt_tod_txled(self):
        """
        Join vmt distribution by vehicle types, time of day vmt distribution, and TxLED emission reduction factors
        to the emisrate table.
        """
        start_time = time.time()
        if self.created_all_indices:
            self.cur.execute("FLUSH TABLES;")
            self.cur.execute(
                f"""
                UPDATE emisrate a
                JOIN vmtmix_weekday_{self.district_abb}_{self.analysis_year_todmix}  b  ON
                a.period = b.period AND
                a.sourcetypeID = b.MOVES_STcode AND
                a.fueltypeID = b.MOVES_FTcode AND
                a.roadtypeID = b.VMX_RDcode
                SET a.stypemix = b.VMTmix;
            """
            )
            self.cur.execute(
                f"""
                UPDATE emisrate a
                JOIN hourmix_running_{self.district_abb} c ON
                a.hourid = c.TOD
                SET a.HourMix = c.factor;
            """
            )
            if self.use_txled:
                self.cur.execute(
                    f"""
                    UPDATE emisrate a
                    LEFT JOIN txled_long_{self.analysis_year} d  ON
                    a.pollutantid = d.pollutantid AND
                    a.sourcetypeid = d.sourcetypeid AND
                    a.fueltypeid = d.fueltypeid
                    SET a.txledfac = d.txled_fac;
                """
                )
                self.cur.execute(
                    f"""
                    UPDATE emisrate
                    SET txledfac = 1.0 WHERE txledfac IS NULL;
                """
                )
            else:
                self.cur.execute(f"""UPDATE emisrate SET txledfac = 1.0;""")
        else:
            print(
                "Run create_indices_before_joins to speed-up joins. Will not run this function unless "
                "create_indices_before_joins ran without errors."
            )
            raise ValueError("self.created_all_indices is still False.")
        print(
            "---join_emisrate_vmt_tod_txled execution time:  %s seconds---"
            % (time.time() - start_time)
        )
        logging.info(
            "---join_emisrate_vmt_tod_txled execution time:  %s seconds---"
            % (time.time() - start_time)
        )

    def compute_factored_emisrate(self):
        """Weight the emission rate by vmt for different vehicle types, fuel times, proportion of vehicles in different
        time of day and if the TxLED program is active in a county (or majority of county of a district."""
        self.cur.execute(
            """UPDATE emisrate SET emisFact = ERate*stypemix*HourMix*txledfac;"""
        )

    def agg_by_rdtype_funcls_avgspd(
        self, add_seperate_conflicted_copy=False, conflicted_copy_suffix=""
    ):
        """
        Aggregate (sum) emission rate by Area, yearid, monthid, funclass, avgspeed. Insert the aggregated table
        to mvs2014b_erlt_out.running_erlt_intermediate if no duplicate exists. Else, ask the user if they want a
        conflicted copy saved in mvs2014_erlt_conflicted schema.
        """
        start_time = time.time()
        cmd_insert = """
                INSERT INTO mvs2014b_erlt_out.running_erlt_intermediate( Area, yearid, monthid, funclass, avgspeed, 
                CO, NOX, SO2, NO2, VOC, CO2EQ, PM10, PM25, BENZ, NAPTH, BUTA, FORM, ACTE, ACROL, ETYB, DPM, POM)
        """
        cmd_create_conflicted = f"""
             CREATE TABLE mvs2014b_erlt_conflicted.running_{self.district_abb}_{self.analysis_year}_{self.anaylsis_month}_{conflicted_copy_suffix}
        """
        cmd_common = """
                SELECT Area,yearid,monthid,funclass,avgspeed,
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
                FROM emisrate
                GROUP BY Area,yearid,monthid,funclass,avgspeed
        """
        try:
            cmd_insert_agg = cmd_insert + cmd_common
            if not add_seperate_conflicted_copy:
                self.cur.execute(cmd_insert_agg)
            else:
                print(
                    f"Saving running emission rate for {self.district_abb}, {self.analysis_year}, "
                    f"{self.anaylsis_month} in mvs2014b_erlt_conflicted for review."
                )
                cmd_create_agg = cmd_create_conflicted + cmd_common
                self.cur.execute(
                    f"""
                    DROP TABLE IF EXISTS mvs2014b_erlt_conflicted.running_{self.district_abb}_{self.analysis_year}_{self.anaylsis_month}_{conflicted_copy_suffix};
                """
                )
                self.cur.execute(cmd_create_agg)
            print(
                "---agg_by_rdtype_funcls_avgspd execution time:  %s seconds---"
                % (time.time() - start_time)
            )
            logging.info(
                "---agg_by_rdtype_funcls_avgspd execution time:  %s seconds---"
                % (time.time() - start_time)
            )
        except mariadb.IntegrityError as integerityrr:
            print(integerityrr)
            print(
                "Re-create the mvs2014b_erlt_out.running_erlt_intermediate table if you want to overwrite it."
            )
            print(
                f"Cannot write over the data in mvs2014b_erlt_out.running_erlt_intermediate. Drop the rows you are"
                f"trying to overwrite"
            )
            raise


if __name__ == "__main__":
    path_to_log_dir = os.path.join(PATH_INTERIM_RUNNING, "Log Files")
    if not os.path.exists(path_to_log_dir):
        os.mkdir(path_to_log_dir)
    path_log_file = os.path.join(path_to_log_dir, "running_test_sql.log")
    logging.basicConfig(filename=path_log_file, filemode="w", level=logging.INFO)
    # ---
    db_nms_list = get_db_nm_list(county_abb="elp")
    db_nm = "mvs14b_erlt_elp_48141_2022_7_cer_out"
    logging.info(f"# Start processing {db_nm}")
    elp_2022_7_obj = RunningSqlCmds(db_nm_=db_nm)
    query_start_time = time.time()
    # elp_2022_7_obj.aggregate_emisrate_rateperdist()
    # hourmix_elp = elp_2022_7_obj.get_hourmix_for_db_district()
    vmt_mix_elp_2022 = (
        elp_2022_7_obj.get_vmtmix_for_db_district_weekday_closest_vmt_yr()
    )
    txled_elp_dict = elp_2022_7_obj.get_txled_for_db_district_year()
    elp_2022_7_obj.create_indices_before_joins()
    elp_2022_7_obj.join_emisrate_vmt_tod_txled()
    elp_2022_7_obj.compute_factored_emisrate()
    elp_2022_7_obj.agg_by_rdtype_funcls_avgspd(
        add_seperate_conflicted_copy=True, conflicted_copy_suffix="drop_after_testing"
    )
    elp_2022_7_obj.close_conn()
    logging.info(
        "---Query execution time:  %s seconds ---" % (time.time() - query_start_time)
    )
    logging.info(f"# End processing {db_nm}")
    del elp_2022_7_obj
