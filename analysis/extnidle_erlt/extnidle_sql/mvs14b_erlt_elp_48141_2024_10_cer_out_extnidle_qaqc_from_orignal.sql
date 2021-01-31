SET SQL_SAFE_UPDATES = 0;

-- Script creates the required base rate table from MOVES output databases 
-- Only required pollutants are selected based on the emissionrate output table

-- Set the analysis District for filtering District in hourmix, TxDOT_Dist in TxDOT_Dist,
-- assigning Area is emisrate table.
SET @analysis_district = "El Paso";
-- Set districts with TxLED program.
SET @txled_prog_disticts = "Austin"; -- Remove El Paso from here---Check with Madhu---It likely doesn't have TxLED.
SELECT FIND_IN_SET(@analysis_district, @txled_prog_disticts);
SET @analysis_year = (SELECT yearid FROM mvs14b_erlt_elp_48141_2024_10_cer_out.ratePerHour LIMIT 1);

FLUSH Tables;
DROP TABLE IF EXISTS mvs14b_erlt_elp_48141_2024_10_cer_out.Extnidlerate;
CREATE TABLE mvs14b_erlt_elp_48141_2024_10_cer_out.Extnidlerate
SELECT yearID, monthid, hourID, pollutantID, sourceTypeID, fuelTypeID, processid,sum(ratePerHour) as rateperhour 
FROM mvs14b_erlt_elp_48141_2024_10_cer_out.rateperhour
WHERE pollutantid in (2,3,31,33,87,98,100,110,20, 23, 185,24,25,26,27,41,68, 69,70,71, 72, 73, 74, 75, 76, 77, 78,
81, 82, 83, 84, 168, 169, 170, 171, 172, 173, 174, 175, 176, 177,178, 181, 182, 183, 184) and sourceTypeID = 62
group by yearID,monthid,hourID, pollutantID, sourceTypeID, fuelTypeID, processid;

-- Script creates the hour-mix table from the MOVES default database
-- Note that default databse need to be present in order to execute this query  
-- If MOVES model version is updated by EPA, MOVES default schema referenced here need to be changed

FLUSH Tables;
DROP TABLE IF EXISTS mvs14b_erlt_elp_48141_2024_10_cer_out.hourmix_extidle;
CREATE TABLE mvs14b_erlt_elp_48141_2024_10_cer_out.hourmix_extidle
SELECT a.hourID,b.hotellingdist from movesdb20181022.hourday a
JOIN movesdb20181022.sourcetypehour b on 
a.hourdayid = b.hourdayid
where a.dayid = '5';

-- Script to get the TxLED Table.
DELIMITER $$
USE mvs14b_erlt_elp_48141_2024_10_cer_out $$
DROP PROCEDURE IF EXISTS import_txled_proc $$
CREATE PROCEDURE import_txled_proc()
BEGIN
   IF FIND_IN_SET(@analysis_district, @txled_prog_disticts)
   THEN
		flush TABLES;
		drop table  if exists mvs14b_erlt_elp_48141_2024_10_cer_out.TxLed_Long_Copy;
		create table mvs14b_erlt_elp_48141_2024_10_cer_out.TxLed_Long_Copy 
		SELECT * FROM txled_db.txled_long
		WHERE yearid = @analysis_year;
				
-- 		ALTER TABLE mvs14b_erlt_elp_48141_2024_10_cer_out.TxLed_Long_Copy
-- 		DROP yearid,
-- 		MODIFY pollutantid SMALLINT,
-- 		MODIFY sourcetypeid SMALLINT,
-- 		MODIFY fueltypeid SMALLINT,
-- 		MODIFY txled_fac FLOAT(6);
   END IF;
END $$
DELIMITER ;
CALL import_txled_proc();

-- Script to add necessary fields to the rate table and populate it with apprropriate data

FLUSH Tables;
ALTER TABLE mvs14b_erlt_elp_48141_2024_10_cer_out.Extnidlerate
ADD COLUMN Hourmix float,
ADD COLUMN Area char(25),
ADD COLUMN Processtype char(25),
ADD COLUMN txledfac FLOAT(6),
ADD COLUMN emisFact float;

FLUSH TABLES;
Update mvs14b_erlt_elp_48141_2024_10_cer_out.Extnidlerate
SET AREA = @analysis_district;

-- Script to add process type group to each MOVES processid

FLUSH TABLES;
UPDATE mvs14b_erlt_elp_48141_2024_10_cer_out.Extnidlerate SET Processtype = "Extnd_Exhaust" WHERE Processid in (17,90);
UPDATE mvs14b_erlt_elp_48141_2024_10_cer_out.Extnidlerate SET Processtype = "APU" WHERE Processid = 91;

-- Script to load TxLED to the base rate table
DELIMITER $$
USE mvs14b_erlt_elp_48141_2024_10_cer_out $$
DROP PROCEDURE IF EXISTS txled_join_proc $$
CREATE PROCEDURE txled_join_proc()
BEGIN
   IF FIND_IN_SET(@analysis_district, @txled_prog_disticts)
   THEN
		CREATE INDEX IF NOT EXISTS extndidx2
		ON mvs14b_erlt_elp_48141_2024_10_cer_out.Extnidlerate 
		(pollutantid, sourcetypeid, fueltypeid);
		
		CREATE INDEX txledidx1
		ON mvs14b_erlt_elp_48141_2024_10_cer_out.TxLed_Long_Copy 
		(pollutantid, sourcetypeid, fueltypeid);
		
		flush tables;
		UPDATE mvs14b_erlt_elp_48141_2024_10_cer_out.Extnidlerate a
		LEFT JOIN mvs14b_erlt_elp_48141_2024_10_cer_out.TxLed_Long_Copy d ON
		a.pollutantid = d.pollutantid AND
		a.sourcetypeid = d.sourcetypeid AND
		a.fueltypeid = d.fueltypeid
		SET a.txledfac = d.txled_fac;
   END IF;
END $$
DELIMITER ;
CALL txled_join_proc();

UPDATE mvs14b_erlt_elp_48141_2024_10_cer_out.Extnidlerate
SET txledfac = 1.0 WHERE txledfac IS NULL;

-- Script to save the final Extended Idle  emission factors table in a desired format on your local drive
-- Note that user has to provide/update the path so that final Extended Idle  emission factor files can be saved in the desired folder

UPDATE mvs14b_erlt_elp_48141_2024_10_cer_out.Extnidlerate a
JOIN mvs14b_erlt_elp_48141_2024_10_cer_out.hourmix_extidle b
ON a.hourID = b.hourID
SET a.Hourmix = b.hotellingdist;

-- Script to calculate composite extended idle emission rate

flush tables;
UPDATE mvs14b_erlt_elp_48141_2024_10_cer_out.Extnidlerate
SET emisFact = rateperhour*HourMix*txledfac;

-- Script to save the final start emission factors table in a desired format on your local drive
-- Note that user has to provide/update the path so that final start emission factor files can be saved in the desired folder

drop table  if exists MVS2014b_ERLT_QAQC.mvs14b_erlt_elp_48141_2024_10_cer_out_extnidle_qaqc_from_orignal;
create table MVS2014b_ERLT_QAQC.mvs14b_erlt_elp_48141_2024_10_cer_out_extnidle_qaqc_from_orignal
SELECT Area,yearid,monthid,Processtype,
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
FROM mvs14b_erlt_elp_48141_2024_10_cer_out.Extnidlerate
GROUP BY Area,yearid,monthid,Processtype
