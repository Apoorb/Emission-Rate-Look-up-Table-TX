SET SQL_SAFE_UPDATES = 0;

-- Script creates the required base rate table from MOVES output databases 
-- Only required pollutants are selected based on the emissionrate output table

flush tables;
drop table  if exists mvs14b_erlt_elp_48141_2022_7_cer_out.emisrate;
create table mvs14b_erlt_elp_48141_2022_7_cer_out.emisrate (SELECT yearid,monthid,hourid,
roadtypeid,pollutantid,sourcetypeid,fueltypeid,avgSpeedBinID,
sum(rateperdistance)as ERate 
FROM mvs14b_erlt_elp_48141_2022_7_cer_out.rateperdistance
WHERE pollutantid in (2,3,31,33,87,98,100,110,20, 23, 185,24,25,26,27,41,68, 69,70,71, 72, 73, 74, 75, 76, 77, 78,
81, 82, 83, 84, 168, 169, 170, 171, 172, 173, 174, 175, 176, 177,178, 181, 182, 183, 184) and processid not in (18,19)
group by yearid,monthid,hourid,roadtypeid,pollutantid,sourcetypeid,fueltypeid,avgSpeedBinID);

-- Script creates the hour-mix table from the MOVES default database
-- Note that default databse need to be present in order to execute this query  
-- If MOVES model version is updated by EPA, MOVES default schema referenced here need to be changes

flush tables;
drop table  if exists mvs14b_erlt_elp_48141_2022_7_cer_out.Hourmix;
create table mvs14b_erlt_elp_48141_2022_7_cer_out.Hourmix 
SELECT * FROM vmtmix_fy20.hourmix
where District = "El Paso";

-- Script creates the VMT-mix table from the movesactivity output table available in the MOVES output databse used for rate development

flush tables;
drop table  if exists mvs14b_erlt_elp_48141_2022_7_cer_out.VMTmix;
create table mvs14b_erlt_elp_48141_2022_7_cer_out.VMTmix 
SELECT * FROM vmtmix_fy20.todmix 
where TxDOT_Dist = "El Paso" and Daytype = "Weekday" and YearID = 2020;

-- Script to add necessary fields to the rate table and populate it with appropriate data

FLUSH TABLES;
ALTER TABLE mvs14b_erlt_elp_48141_2022_7_cer_out.emisrate
ADD COLUMN avgspeed float(3,1),
ADD COLUMN Hourmix float,
ADD COLUMN TxLED float,
ADD COLUMN stypemix float,
ADD COLUMN emisFact float,
ADD COLUMN Funclass char(25),
ADD COLUMN Period char(2),
ADD COLUMN Area char(25);

FLUSH TABLES;
Update mvs14b_erlt_elp_48141_2022_7_cer_out.emisrate
SET Area = "El Paso";

FLUSH TABLES;
Update mvs14b_erlt_elp_48141_2022_7_cer_out.emisrate
SET TxLED = 1.00;

-- Script to add speeds to each MOVES avgspeedbinid

FLUSH TABLES;
UPDATE mvs14b_erlt_elp_48141_2022_7_cer_out.emisrate
SET avgspeed = (avgSpeedBinID - 1) * 5;

FLUSH TABLES;
UPDATE mvs14b_erlt_elp_48141_2022_7_cer_out.emisrate
SET avgspeed = 2.5
WHERE avgSpeedBinID = 1;

-- Script to add roadtype description to each MOVES roadtypeid

FLUSH TABLES;
UPDATE mvs14b_erlt_elp_48141_2022_7_cer_out.emisrate SET Funclass = "Rural-Freeway" WHERE roadtypeid = 2;

UPDATE mvs14b_erlt_elp_48141_2022_7_cer_out.emisrate SET Funclass = "Rural-Arterial" WHERE roadtypeid = 3;

UPDATE mvs14b_erlt_elp_48141_2022_7_cer_out.emisrate SET Funclass = "Urban-Freeway" WHERE roadtypeid = 4;

UPDATE mvs14b_erlt_elp_48141_2022_7_cer_out.emisrate SET Funclass = "Urban-Arterial" WHERE roadtypeid = 5;

FLUSH TABLES;
UPDATE mvs14b_erlt_elp_48141_2022_7_cer_out.emisrate
SET period = "AM" WHERE hourid in (7,8,9);

FLUSH TABLES;
UPDATE mvs14b_erlt_elp_48141_2022_7_cer_out.emisrate
SET period = "PM" WHERE hourid in (17,18,19);

FLUSH TABLES;
UPDATE mvs14b_erlt_elp_48141_2022_7_cer_out.emisrate
SET period = "MD" WHERE hourid in (10,11,12,13,14,15,16);

FLUSH TABLES;
UPDATE mvs14b_erlt_elp_48141_2022_7_cer_out.emisrate
SET period = "ON" WHERE hourid in (1,2,3,4,5,6,20,21,22,23,24);

-- Script to load VMT-mix by vehicle type to the base rate table
	
CREATE INDEX efidx1
ON mvs14b_erlt_elp_48141_2022_7_cer_out.emisrate 
(Period, sourcetypeid, fueltypeid, roadtypeid);

flush tables;
UPDATE mvs14b_erlt_elp_48141_2022_7_cer_out.emisrate a
JOIN mvs14b_erlt_elp_48141_2022_7_cer_out.VMTmix  b  ON
a.period = b.period AND
a.sourcetypeID = b.MOVES_STcode AND
a.fueltypeID = b.MOVES_FTcode AND
a.roadtypeID = b.VMX_RDcode
SET a.stypemix = b.VMTmix;

-- Script to load hour-mix to the base rate table    

flush tables;
UPDATE mvs14b_erlt_elp_48141_2022_7_cer_out.emisrate a
JOIN mvs14b_erlt_elp_48141_2022_7_cer_out.HourMix c ON
a.hourid = c.TOD
SET a.HourMix = c.factor;

-- Script to create composite emission rate

flush tables;
UPDATE mvs14b_erlt_elp_48141_2022_7_cer_out.emisrate
SET emisFact = ERate*stypemix*HourMix*TxLED;


drop table  if exists MVS2014b_ERLT_QAQC.mvs14b_erlt_elp_48141_2022_7_cer_out_qaqc_from_orignal;
create table MVS2014b_ERLT_QAQC.mvs14b_erlt_elp_48141_2022_7_cer_out_qaqc_from_orignal 
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
FROM mvs14b_erlt_elp_48141_2022_7_cer_out.emisrate
GROUP BY Area,yearid,monthid,funclass,avgspeed

