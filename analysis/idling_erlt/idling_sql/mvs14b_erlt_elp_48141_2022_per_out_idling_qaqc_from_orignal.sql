SET SQL_SAFE_UPDATES = 0;

-- Set the analysis District for filtering District in hourmix, TxDOT_Dist in TxDOT_Dist,
-- assigning Area is emisrate table.
SET @analysis_district = "El Paso";
-- Set districts with TxLED program.
SET @txled_prog_disticts = "Austin,District X,District Y"; -- Remove El Paso from here---Check with Madhu---It likely doesn't have TxLED.
SELECT FIND_IN_SET(@analysis_district, @txled_prog_disticts);
SET @analysis_year = (SELECT yearid FROM mvs14b_erlt_elp_48141_2022_per_out.movesoutput LIMIT 1);


flush tables;
drop table  if exists mvs14b_erlt_elp_48141_2022_per_out.idlerate;
create table mvs14b_erlt_elp_48141_2022_per_out.idlerate (SELECT yearid, monthid,hourid,countyid,
linkid,pollutantid,sourcetypeid,fueltypeid,sum(emissionquant)as emission 
FROM mvs14b_erlt_elp_48141_2022_per_out.movesoutput
group by yearid,monthid,hourid,countyid,roadtypeid,linkid,pollutantid,sourcetypeid,fueltypeid);

DELIMITER $$
USE mvs14b_erlt_elp_48141_2022_7_cer_out $$
DROP PROCEDURE IF EXISTS find_todmix_yr $$
CREATE PROCEDURE find_todmix_yr()
BEGIN
	IF @analysis_year <= 2022 THEN 
		SET @analysis_year_todmix = 2020;
	ELSEIF @analysis_year > 2022 AND @analysis_year <= 2027 THEN 
		SET @analysis_year_todmix = 2025;
	ELSEIF @analysis_year > 2027 AND @analysis_year <= 2032 THEN 
		SET @analysis_year_todmix = 2030;
	ELSEIF @analysis_year > 2032 AND @analysis_year <= 2037 THEN 
		SET @analysis_year_todmix = 2035;
	ELSEIF @analysis_year > 2037 AND @analysis_year <= 2042 THEN 
		SET @analysis_year_todmix = 2040;
	ELSEIF @analysis_year > 2042 AND @analysis_year <= 2047 THEN 
		SET @analysis_year_todmix = 2045;
	ELSEIF @analysis_year > 2047 AND @analysis_year <= 2052 THEN 
		SET @analysis_year_todmix = 2050;
	END IF;
END $$
DELIMITER ;
CALL find_todmix_yr;
SELECT @analysis_year, @analysis_year_todmix;

-- Script to get the TxLED Table.
DELIMITER $$
USE mvs14b_erlt_elp_48141_2022_per_out $$
DROP PROCEDURE IF EXISTS import_txled_proc $$
CREATE PROCEDURE import_txled_proc()
BEGIN
   IF FIND_IN_SET(@analysis_district, @txled_prog_disticts)
   THEN
		flush TABLES;
		drop table  if exists mvs14b_erlt_elp_48141_2022_per_out.TxLed_Long_Copy;
		create table mvs14b_erlt_elp_48141_2022_per_out.TxLed_Long_Copy 
		SELECT * FROM txled_db.txled_long
		WHERE yearid = @analysis_year;
   END IF;
END $$
DELIMITER ;
CALL import_txled_proc();

FLUSH TABLES;
ALTER TABLE mvs14b_erlt_elp_48141_2022_per_out.idlerate
ADD COLUMN stypemix float,
ADD COLUMN idlerate float,
ADD COLUMN period char(2),
ADD COLUMN Area char(25),
ADD COLUMN VMTmix FLOAT,
ADD COLUMN txledfac FLOAT(6),
ADD COLUMN emisfact FLOAT;


UPDATE mvs14b_erlt_elp_48141_2022_per_out.idlerate
SET AREA = @analysis_district;
            
FLUSH TABLES;
UPDATE mvs14b_erlt_elp_48141_2022_per_out.idlerate
SET period = "AM" WHERE hourid = 8;

FLUSH TABLES;
UPDATE mvs14b_erlt_elp_48141_2022_per_out.idlerate
SET period = "MD" WHERE hourid = 15;

FLUSH TABLES;
UPDATE mvs14b_erlt_elp_48141_2022_per_out.idlerate
SET period = "PM" WHERE hourid = 18;

FLUSH TABLES;
UPDATE mvs14b_erlt_elp_48141_2022_per_out.idlerate
SET period = "ON" WHERE hourid = 23;

flush tables;
drop table  if exists mvs14b_erlt_elp_48141_2022_per_out.houridlemix;
create table mvs14b_erlt_elp_48141_2022_per_out.houridlemix (SELECT yearid,monthid,hourid,
linkid,sourcetypeid,fueltypeid,sum(activity)as vmx 
FROM mvs14b_erlt_elp_48141_2022_per_out.movesactivityoutput
where activitytypeid = 4 and activity > 0
group by yearid,monthid,hourid,countyid,linkid,sourcetypeid,fueltypeid);


CREATE INDEX IF NOT EXISTS idleidx1
ON idlerate (monthid, hourid, linkid, sourcetypeid, fueltypeid);

CREATE INDEX IF NOT EXISTS  houridlemix_idx1 
ON houridlemix (monthid, hourid, linkid, sourcetypeid, fueltypeid);
                
FLUSH TABLES;
UPDATE mvs14b_erlt_elp_48141_2022_per_out.idlerate A
JOIN mvs14b_erlt_elp_48141_2022_per_out.houridlemix B ON
(A.monthid = B.monthid AND
A.hourid = B.hourid AND
A.linkid = B.linkid AND
A.sourcetypeid = B.sourcetypeid AND
A.fueltypeid = B.fueltypeid)
SET A.stypemix = B.vmx;

FLUSH TABLES;
UPDATE mvs14b_erlt_elp_48141_2022_per_out.idlerate
SET idlerate = emission/stypemix ;

flush tables;
drop table  if exists mvs14b_erlt_elp_48141_2022_per_out.SUTmix;
create table mvs14b_erlt_elp_48141_2022_per_out.SUTmix 
SELECT * FROM vmtmix_fy20.todmix 
where TxDOT_Dist = @analysis_district and Daytype = "Weekday" and YearID = @analysis_year_todmix and VMX_RDcode = 5;

CREATE INDEX IF NOT EXISTS idleidx2 
ON idlerate (period, sourcetypeID, fueltypeID);

CREATE INDEX IF NOT EXISTS sutmixidx1 
ON SUTmix (period, MOVES_STcode, MOVES_FTcode);

flush tables;
UPDATE mvs14b_erlt_elp_48141_2022_per_out.idlerate a
JOIN mvs14b_erlt_elp_48141_2022_per_out.SUTmix  b ON
a.period = b.period AND
a.sourcetypeID = b.MOVES_STcode AND
a.fueltypeID = b.MOVES_FTcode 
SET a.VMTmix = b.VMTmix;

-- Script to load TxLED to the base rate table
DELIMITER $$
USE mvs14b_erlt_elp_48141_2022_per_out $$
DROP PROCEDURE IF EXISTS txled_join_proc $$
CREATE PROCEDURE txled_join_proc()
BEGIN
   IF FIND_IN_SET(@analysis_district, @txled_prog_disticts)
   THEN
		CREATE INDEX IF NOT EXISTS extndidx2
		ON mvs14b_erlt_elp_48141_2022_per_out.idlerate 
		(pollutantid, sourcetypeid, fueltypeid);
		
		CREATE INDEX txledidx1
		ON mvs14b_erlt_elp_48141_2022_per_out.TxLed_Long_Copy 
		(pollutantid, sourcetypeid, fueltypeid);
		
		flush tables;
		UPDATE mvs14b_erlt_elp_48141_2022_per_out.idlerate a
		LEFT JOIN mvs14b_erlt_elp_48141_2022_per_out.TxLed_Long_Copy d ON
		a.pollutantid = d.pollutantid AND
		a.sourcetypeid = d.sourcetypeid AND
		a.fueltypeid = d.fueltypeid
		SET a.txledfac = d.txled_fac;
   END IF;
END $$
DELIMITER ;
CALL txled_join_proc();

UPDATE mvs14b_erlt_elp_48141_2022_per_out.idlerate
SET txledfac = 1.0 WHERE txledfac IS NULL;

FLUSH TABLES;
UPDATE mvs14b_erlt_elp_48141_2022_per_out.idlerate
SET emisfact = idlerate*VMTmix*txledfac;

drop table  if exists MVS2014b_ERLT_QAQC.mvs14b_erlt_elp_48141_2022_per_out_idling_qaqc_from_orignal;
create table MVS2014b_ERLT_QAQC.mvs14b_erlt_elp_48141_2022_per_out_idling_qaqc_from_orignal
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
FROM mvs14b_erlt_elp_48141_2022_per_out.idlerate
GROUP BY Area,yearid,monthid,hourid,period;