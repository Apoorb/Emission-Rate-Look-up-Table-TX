SET SQL_SAFE_UPDATES = 0;

-- Script creates the required base rate table from MOVES output databases 
-- Only required pollutants are selected based on the emissionrate output table

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

-- Script to add necessary fields to the rate table and populate it with apprropriate data

FLUSH TABLES;
ALTER TABLE mvs14b_erlt_elp_48141_2020_1_cer_out.startrate
ADD COLUMN Hourmix float,
ADD COLUMN emisFact float,
ADD COLUMN Area char(25),
ADD COLUMN VehicleType char(50),
ADD COLUMN FuelType char(10);

ALTER TABLE mvs14b_erlt_elp_48141_2020_1_cer_out.hourmix_starts
ADD COLUMN sumact float,
ADD COLUMN hrmix float;

FLUSH TABLES;
Update mvs14b_erlt_elp_48141_2020_1_cer_out.startrate
SET Area = "El Paso";

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
SET emisFact = ERate*HourMix;


-- Script to save the final start emission factors table in a desired format on your local drive
-- Note that user has to provide/update the path so that final start emission factor files can be saved in the desired folder

Select "AREA","YEAR", "MONTHID", "VEHICLETYPE", "FUELTYPE",
"CO", "NOX","SO2", "NO2", "VOC", "CO2EQ","PM10","PM25"
Union all
SELECT Area,yearid,monthid,VehicleType,FUELTYPE,
SUM(IF(pollutantid = 2, emisfact, 0)) AS CO,
SUM(IF(pollutantid = 3, emisfact, 0)) AS NOX,
SUM(IF(pollutantid = 31, emisfact, 0)) AS SO2,
SUM(IF(pollutantid = 33, emisfact, 0)) AS NO2,
SUM(IF(pollutantid = 87, emisfact, 0)) AS VOC,
SUM(IF(pollutantid = 98, emisfact, 0)) AS CO2EQ,
SUM(IF(pollutantid = 100, emisfact, 0)) AS PM10,
SUM(IF(pollutantid = 110, emisfact, 0)) AS PM25
FROM mvs14b_erlt_elp_48141_2020_1_cer_out.startrate
GROUP BY Area,yearid,monthid,VehicleType,FUELTYPE
INTO OUTFILE 'C:/Users/A-Bibeka/Downloads/ElP_2020_Start_Criteria&GHG_ERLT.csv'
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\r\n';

FLUSH TABLES;
Select "AREA","YEAR", "MONTHID", "VEHICLETYPE", "FUELTYPE",
"BENZ", "NAPTH","BUTA", "FORM", "ACTE","ACROL","ETYB", "DPM","POM"
Union all
SELECT Area,yearid,monthid,VehicleType,FUELTYPE,
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
INTO OUTFILE 'D:/Projects/ERLT/Output/ELpaso/ElP_2020_Start_MSATS_ERLT.csv'
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\r\n';