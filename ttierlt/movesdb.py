import pandas as pd
from ttierlt.utils import connect_to_server_db


class MovesDb:
    def __init__(self, db_nm_):
        self.moves2014b_db_nm = "movesdb20181022"
        # ref: https://www.tceq.texas.gov/assets/public/implementation/air/sip/texled/TXLED_Map.pdf
        self.MAP_DISTRICT_ABB_FULL_NM_TXLED = {
            "elp": {"area_district": "El Paso", "txled_active": False},
            "aus": {"area_district": "Austin", "txled_active": True},
            "bmt": {"area_district": "Beaumont", "txled_active": True},
            "crp": {
                "area_district": "Corpus Christi",
                "txled_active": True,
            },  # Multiple counties in crp have TxLED.
            "dal": {"area_district": "Dallas", "txled_active": True},
            "ftw": {
                "area_district": "Fort Worth",
                "txled_active": True,
            },  # Multiple counties in ftw have TxLED.
            "hou": {"area_district": "Houston", "txled_active": True},
            "wac": {"area_district": "Waco", "txled_active": True},
            "sat": {"area_district": "San Antonio", "txled_active": True},
        }
        self.MAP_RD_TYPE = {
            2: "Rural-Freeway",
            3: "Rural-Arterial",
            4: "Urban-Freeway",
            5: "Urban-Arterial",
        }
        # TODO: Might change the time assignment for El Paso
        self.MAP_PERIOD_HOURID = {
            "AM": (7, 8, 9),
            "PM": (17, 18, 19),
            "MD": (10, 11, 12, 13, 14, 15, 16),
            "ON": (1, 2, 3, 4, 5, 6, 20, 21, 22, 23, 24),
        }
        self.db_nm = db_nm_
        self.db_nm_county_year_month_dict = {
            "county": db_nm_.split("_")[2],
            "fips": db_nm_.split("_")[3],
            "year": db_nm_.split("_")[4],
            "month_id": db_nm_.split("_")[5],
        }
        """
        Example: mvs14b_erlt_elp_48141_2020_1_cer_out can be decomposed as follows:
            mvs14b: MOVES 2014---index 0
            erlt: Project name; emission rate look-up table---index 1
            elp: El-Passo---index 2
            48141: FIPS code for El-Passo County---index 3
            2020: Year---index 4
            1: Month; Jan---index 5
            cer: Garbage
            out: Garbage
        """
        self.analysis_year = int(self.db_nm_county_year_month_dict["year"])
        self.anaylsis_month = int(self.db_nm_county_year_month_dict["month_id"])
        self.district_abb = self.db_nm_county_year_month_dict["county"]
        self.area_district = self.MAP_DISTRICT_ABB_FULL_NM_TXLED[self.district_abb][
            "area_district"
        ]
        self.use_txled = self.MAP_DISTRICT_ABB_FULL_NM_TXLED[self.district_abb][
            "txled_active"
        ]
        self.analysis_year_todmix = None
        self.get_tdmix_year()
        # Connect to sql.
        self.conn = connect_to_server_db(database_nm=self.db_nm)
        self.cur = self.conn.cursor()
        # SQL Housekeeping. Set DB specific variables in SQL.
        self.sql_housekeeping()
        self.txled_df = pd.DataFrame()

    def get_tdmix_year(self):
        if self.analysis_year > 2017:
            if self.analysis_year <= 2022:
                self.analysis_year_todmix = 2020
            elif self.analysis_year <= 2027:
                self.analysis_year_todmix = 2025
            elif self.analysis_year <= 2032:
                self.analysis_year_todmix = 2030
            elif self.analysis_year <= 2037:
                self.analysis_year_todmix = 2035
            elif self.analysis_year <= 2042:
                self.analysis_year_todmix = 2040
            elif self.analysis_year <= 2047:
                self.analysis_year_todmix = 2045
            elif self.analysis_year <= 2052:
                self.analysis_year_todmix = 2050
            else:
                raise ValueError("Analysis year is out of bounds; over 2052.")
        else:
            raise ValueError("Analysis year is out of bounds; under 2017.")

    def sql_housekeeping(self):
        self.cur.execute("SET SQL_SAFE_UPDATES = 0;")
        self.cur.execute(f"SET @analysis_year = {self.analysis_year};")
        self.cur.execute(f"SET @analysis_year_todmix = {self.analysis_year_todmix};")
        self.cur.execute(f"SET @analysis_district = '{self.area_district}';")
        self.test_sql_housekeeping()

    def test_sql_housekeeping(self):
        self.cur.execute(f"SELECT @analysis_year;")
        test_analysis_yr = self.cur.fetchone()[0]
        self.cur.execute(f"SELECT @analysis_year_todmix;")
        test_analysis_yr_todmix = self.cur.fetchone()[0]
        self.cur.execute(f"SELECT @analysis_district;")
        test_analysis_district = self.cur.fetchone()[0]
        assert (
            test_analysis_yr == self.analysis_year
        ), "@analysis_year variable is not correctly set in MariaDB."
        assert abs(test_analysis_yr_todmix - self.analysis_year) <= 2, (
            "Check the formula for analysis year for todmix."
            "It should be within +-2 years on the analysis "
            "year"
        )
        assert test_analysis_district == self.area_district, (
            "@analysis_district variable is not correctly set " "in MariaDB."
        )

    def get_txled_for_db_district_year(self):
        """
        Get the TxLED factors by year from the txled_db.txled_long table. Use use_txled
        to determine if a county/ district has TxLED program. Use @analysis_year to
        filter TxLed factor years to only the analysis year.
        Parameters
        ----------
        Returns
        -------
        dict
            Returns dict with { entire txled data and the year in the sql table.
        """
        if self.use_txled:
            self.cur.execute("FLUSH TABLES;")
            self.cur.execute(f"DROP TABLE  IF EXISTS txled_long_{self.analysis_year};")
            self.cur.execute(
                f"""
                CREATE TABLE txled_long_{self.analysis_year} 
                SELECT * FROM txled_db.txled_long
                WHERE yearid = @analysis_year;
            """
            )
            self.cur.execute(
                f"SELECT DISTINCT yearid FROM txled_long_{self.analysis_year};"
            )
            txled_yearid_from_sql_table = self.cur.fetchone()[0]
            self.test_txled_cor_year_pulled(txled_yearid_from_sql_table)
            # Reduce the TxLed table size.
            self.cur.execute(
                f"""
                ALTER TABLE txled_long_{self.analysis_year}
                DROP yearid,
                MODIFY pollutantid SMALLINT,
                MODIFY sourcetypeid SMALLINT,
                MODIFY fueltypeid SMALLINT,
                MODIFY txled_fac FLOAT(6);
            """
            )
            self.txled_df = pd.read_sql(
                f"SELECT * FROM  txled_long_{self.analysis_year} ",
                self.conn,
            )
            self.test_txled_df_is_read()
            return {"txled_df": self.txled_df, "txled_yr": txled_yearid_from_sql_table}

    def test_txled_cor_year_pulled(self, txled_yr):
        """Check if the year matches for TxLED and the MOVES database under
        processing."""
        assert txled_yr == self.analysis_year, (
            "Compare the self.analysis_year with yearid in self.txled. See why "
            "there is a mismatch."
        )

    def test_txled_df_is_read(self):
        """Test if there is data in TxLED table."""
        assert len(self.txled_df) >= 1, (
            "No data in txled table. Compare  the self.analysis_year with yearid "
            "in self.txled"
        )

    def close_conn(self):
        self.conn.close()
