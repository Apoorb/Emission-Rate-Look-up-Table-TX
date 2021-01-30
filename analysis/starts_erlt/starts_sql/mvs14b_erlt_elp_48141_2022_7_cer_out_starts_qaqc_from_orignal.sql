SET SQL_SAFE_UPDATES = 0;

-- Script creates the required base rate table from MOVES output databases 
-- Only required pollutants are selected based on the emissionrate output table

-- Set the analysis District for filtering District in hourmix, TxDOT_Dist in TxDOT_Dist,
-- assigning Area is emisrate table.
SET @analysis_district = "El Paso";
-- Set districts with TxLED program.
SET @txled_prog_disticts = "NOOOOO El Paso,District X"; -- Remove El Paso from here---Check with Madhu---It likely doesn't have TxLED.
SELECT FIND_IN_SET(@analysis_district, @txled_prog_disticts);

flush tables;
Drop table  if exists mvs14b_erlt_elp_48141_2020_1_cer_out.startrate;
Create table mvs14b_erlt_elp_48141_2020_1_cer_out.startrate (SELECT yearid, monthid,hourid,
pollutantid,sourcetypeid,fueltypeid,sum(rateperstart)as ERate 
FROM mvs14b_erlt_elp_48141_2020_1_cer_out.rateperstart
Where pollutantid in (2,3,31,33,87,98,100,110,20, 23, 185,24,25,26,27,41,68, 69,70,71, 72, 73, 74, 75, 76, 77, 78,
81, 82, 83, 84, 168, 169, 170, 171, 172, 173, 174, 175, 176, 177,178, 181, 182, 183, 184)
Group by yearid,monthid,hourid,pollutantid,sourcetypeid,fueltypeid);

-- Script creates the start-mix table from the movesactivity output table available in the MOVES output supplied for rate development

FLUSH TABLES;
drop table  if exists mvs14b_erlt_elp_48141_2020_1_cer_out.hourmix_starts;
create table mvs14b_erlt_elp_48141_2020_1_cer_out.hourmix_starts
SELECT hourID,sourceTypeID,fuelTypeID, startsPerVehicle FROM mvs14b_erlt_elp_48141_2020_1_cer_out.startspervehicle;

-- Script to get the TxLED Table.
DELIMITER $$
USE mvs14b_erlt_elp_48141_2020_1_cer_out $$
DROP PROCEDURE IF EXISTS import_txled_proc $$
CREATE PROCEDURE import_txled_proc()
BEGIN
   IF FIND_IN_SET(@analysis_district, @txled_prog_disticts)
   THEN
		flush TABLES;
		drop table  if exists mvs14b_erlt_elp_48141_2020_1_cer_out.TxLed_Long_Copy;
		create table mvs14b_erlt_elp_48141_2020_1_cer_out.TxLed_Long_Copy 
		SELECT * FROM txled_db.txled_long
		WHERE yearid = @analysis_year;
				
		ALTER TABLE mvs14b_erlt_elp_48141_2020_1_cer_out.TxLed_Long_Copy
		DROP yearid,
		MODIFY pollutantid SMALLINT,
		MODIFY sourcetypeid SMALLINT,
		MODIFY fueltypeid SMALLINT,
		MODIFY txled_fac FLOAT(6);
   END IF;
END $$
DELIMITER ;
CALL import_txled_proc();


-- Script to add necessary fields to the rate table and populate it with apprropriate data

FLUSH TABLES;
ALTER TABLE mvs14b_erlt_elp_48141_2020_1_cer_out.startrate
ADD COLUMN Hourmix float,
ADD COLUMN emisFact float,
ADD COLUMN Area char(25),
ADD COLUMN VehicleType char(50),
ADD COLUMN FuelType char(10),
ADD COLUMN txledfac FLOAT(6);

ALTER TABLE mvs14b_erlt_elp_48141_2020_1_cer_out.hourmix_starts
ADD COLUMN sumact float,
ADD COLUMN hrmix float;

FLUSH TABLES;
Update mvs14b_erlt_elp_48141_2020_1_cer_out.startrate
SET AREA = @analysis_district;

FLUSH TABLES;
Update mvs14b_erlt_elp_48141_2020_1_cer_out.startrate
SET FuelType = "Gasoline" Where FuelTypeID = 1;

FLUSH TABLES;
Update mvs14b_erlt_elp_48141_2020_1_cer_out.startrate
SET FuelType = "Diesel" Where FuelTypeID = 2;

-- Script to add vehicle type group to each MOVES sourcetypeid

flush tables;
UPDATE mvs14b_erlt_elp_48141_2020_1_cer_out.startrate a
JOIN movesdb20181022.sourceusetype  b  ON
a.sourceTypeID = b.sourceTypeID 
SET a.VehicleType = b.sourceTypeName;

-- Script to calculate hour-mix 

flush tables;
UPDATE
mvs14b_erlt_elp_48141_2020_1_cer_out.hourmix_starts as r
JOIN
        ( SELECT   sourceTypeID, fuelTypeID, SUM(startsPerVehicle) as sumact
			FROM mvs14b_erlt_elp_48141_2020_1_cer_out.hourmix_starts a 
				   GROUP BY sourceTypeID, fuelTypeID
        ) AS grp
       ON                   
					  r.sourceTypeID = grp.sourceTypeID  and
					  r.fuelTypeID = grp.fuelTypeID
					  
SET  r.sumact = grp.sumact;

flush tables;
Update mvs14b_erlt_elp_48141_2020_1_cer_out.hourmix_starts
SET hrmix = startsPerVehicle / sumact;
 
-- Script to load hour-mix to the base rate table   
CREATE INDEX IF NOT EXISTS stridx1 ON startrate (hourID, sourcetypeid, fueltypeid);
CREATE INDEX IF NOT EXISTS hrmixdx1 ON hourmix_starts (hourID, sourcetypeid, fueltypeid);
-- Script to load TxLED to the base rate table
DELIMITER $$
USE mvs14b_erlt_elp_48141_2020_1_cer_out $$
DROP PROCEDURE IF EXISTS txled_join_proc $$
CREATE PROCEDURE txled_join_proc()
BEGIN
   IF FIND_IN_SET(@analysis_district, @txled_prog_disticts)
   THEN
		CREATE INDEX IF NOT EXISTS stridx2
		ON mvs14b_erlt_elp_48141_2020_1_cer_out.startrate 
		(pollutantid, sourcetypeid, fueltypeid);
		
		CREATE INDEX txledidx1
		ON mvs14b_erlt_elp_48141_2020_1_cer_out.TxLed_Long_Copy 
		(pollutantid, sourcetypeid, fueltypeid);
		
		flush tables;
		UPDATE mvs14b_erlt_elp_48141_2020_1_cer_out.startrate a
		LEFT JOIN mvs14b_erlt_elp_48141_2020_1_cer_out.TxLed_Long_Copy d ON
		a.pollutantid = d.pollutantid AND
		a.sourcetypeid = d.sourcetypeid AND
		a.fueltypeid = d.fueltypeid
		SET a.txledfac = d.txled_fac;
   END IF;
END $$
DELIMITER ;
CALL txled_join_proc();

UPDATE mvs14b_erlt_elp_48141_2020_1_cer_out.startrate
SET txledfac = 1.0 WHERE txledfac IS NULL;


flush tables;
UPDATE mvs14b_erlt_elp_48141_2020_1_cer_out.startrate a
JOIN mvs14b_erlt_elp_48141_2020_1_cer_out.hourmix_starts  b  ON
a.hourID = b.hourID and
a.sourcetypeID = b.sourcetypeID and
a.fueltypeID = b.fueltypeID 
SET a.Hourmix = b.hrmix;	 


-- Script to calculate composite start emission rate

flush tables;
UPDATE mvs14b_erlt_elp_48141_2020_1_cer_out.startrate
SET emisFact = ERate*HourMix*txledfac;


-- Script to save the final start emission factors table in a desired format on your local drive
-- Note that user has to provide/update the path so that final start emission factor files can be saved in the desired folder

drop table  if exists MVS2014b_ERLT_QAQC.mvs14b_erlt_elp_48141_2020_1_cer_out_starts_qaqc_from_orignal;
create table MVS2014b_ERLT_QAQC.mvs14b_erlt_elp_48141_2020_1_cer_out_starts_qaqc_from_orignal
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
FROM mvs14b_erlt_elp_48141_2020_1_cer_out.startrate
GROUP BY Area,yearid,monthid,VehicleType,FUELTYPE