SET SQL_SAFE_UPDATES = 0;

-- Script creates the required base rate table from MOVES output databases 
-- Only required pollutants are selected based on the emissionrate output table

FLUSH Tables;
DROP TABLE IF EXISTS mvs14b_erlt_elp_48141_2020_1_cer_out.Extnidlerate;
CREATE TABLE mvs14b_erlt_elp_48141_2020_1_cer_out.Extnidlerate
SELECT yearID, monthid, hourID, pollutantID, sourceTypeID, fuelTypeID, processid,sum(ratePerHour) as rateperhour 
FROM mvs14b_erlt_elp_48141_2020_1_cer_out.rateperhour
WHERE pollutantid in (2,3,31,33,87,98,100,110,20, 23, 185,24,25,26,27,41,68, 69,70,71, 72, 73, 74, 75, 76, 77, 78,
81, 82, 83, 84, 168, 169, 170, 171, 172, 173, 174, 175, 176, 177,178, 181, 182, 183, 184) and sourceTypeID = 62
group by yearID,monthid,hourID, pollutantID, sourceTypeID, fuelTypeID, processid;

-- Script creates the hour-mix table from the MOVES default database
-- Note that default databse need to be present in order to execute this query  
-- If MOVES model version is updated by EPA, MOVES default schema referenced here need to be changed

FLUSH Tables;
DROP TABLE IF EXISTS mvs14b_erlt_elp_48141_2020_1_cer_out.hourmix_extidle;
CREATE TABLE mvs14b_erlt_elp_48141_2020_1_cer_out.hourmix_extidle
SELECT a.hourID,b.hotellingdist from movesdb20181022.hourday a
JOIN movesdb20181022.sourcetypehour b on 
a.hourdayid = b.hourdayid
where a.dayid = '5';

-- Script to add necessary fields to the rate table and populate it with apprropriate data

FLUSH Tables;
ALTER TABLE mvs14b_erlt_elp_48141_2020_1_cer_out.Extnidlerate
ADD COLUMN Hourmix float,
ADD COLUMN Area char(25),
ADD COLUMN Processtype char(25),
ADD COLUMN emisFact float;

FLUSH TABLES;
Update mvs14b_erlt_elp_48141_2020_1_cer_out.Extnidlerate
SET Area = "El Paso";

-- Script to add process type group to each MOVES processid

FLUSH TABLES;
UPDATE mvs14b_erlt_elp_48141_2020_1_cer_out.Extnidlerate SET Processtype = "Extnd_Exhaust" WHERE Processid in (17,90);
UPDATE mvs14b_erlt_elp_48141_2020_1_cer_out.Extnidlerate SET Processtype = "APU" WHERE Processid = 91;


-- Script to save the final Extended Idle  emission factors table in a desired format on your local drive
-- Note that user has to provide/update the path so that final Extended Idle  emission factor files can be saved in the desired folder

UPDATE mvs14b_erlt_elp_48141_2020_1_cer_out.Extnidlerate a
JOIN mvs14b_erlt_elp_48141_2020_1_cer_out.hourmix_extidle b
ON a.hourID = b.hourID
SET a.Hourmix = b.hotellingdist;

-- Script to calculate composite extended idle emission rate

flush tables;
UPDATE mvs14b_erlt_elp_48141_2020_1_cer_out.Extnidlerate
SET emisFact = rateperhour*HourMix;

-- Script to save the final start emission factors table in a desired format on your local drive
-- Note that user has to provide/update the path so that final start emission factor files can be saved in the desired folder

Select "YEAR", "MONTHID", "PROCESSTYPE",
"CO", "NOX","SO2", "NO2", "VOC", "CO2EQ","PM10","PM25"
Union all
SELECT yearid,monthid,Processtype,
SUM(IF(pollutantid = 2, emisfact, 0)) AS CO,
SUM(IF(pollutantid = 3, emisfact, 0)) AS NOX,
SUM(IF(pollutantid = 31, emisfact, 0)) AS SO2,
SUM(IF(pollutantid = 33, emisfact, 0)) AS NO2,
SUM(IF(pollutantid = 87, emisfact, 0)) AS VOC,
SUM(IF(pollutantid = 98, emisfact, 0)) AS CO2EQ,
SUM(IF(pollutantid = 100, emisfact, 0)) AS PM10,
SUM(IF(pollutantid = 110, emisfact, 0)) AS PM25
FROM mvs14b_erlt_elp_48141_2020_1_cer_out.Extnidlerate
GROUP BY yearid,monthid,Processtype
INTO OUTFILE 'D:/Projects/ERLT/Output/ELpaso/ElP_2020_Extdle_Criteria&GHG_ERLT.csv'
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\r\n';

FLUSH TABLES;
Select "YEAR", "MONTHID", "PROCESSTYPE",
"BENZ", "NAPTH","BUTA", "FORM", "ACTE","ACROL","ETYB", "DPM","POM"
Union all
SELECT yearid,monthid,Processtype,
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
FROM mvs14b_erlt_elp_48141_2020_1_cer_out.Extnidlerate
GROUP BY yearid,monthid,Processtype
INTO OUTFILE 'D:/Projects/ERLT/Output/ELpaso/ElP_2020_Extdle_MSATS_ERLT.csv'
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\r\n';