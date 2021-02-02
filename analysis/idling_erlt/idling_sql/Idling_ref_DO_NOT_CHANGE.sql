SET SQL_SAFE_UPDATES = 0;

flush tables;
drop table  if exists mvs14b_erlt_elp_48141_2020_per_out.idlerate;
create table mvs14b_erlt_elp_48141_2020_per_out.idlerate (SELECT yearid, monthid,hourid,countyid,
linkid,pollutantid,sourcetypeid,fueltypeid,sum(emissionquant)as emission 
FROM mvs14b_erlt_elp_48141_2020_per_out.movesoutput
group by yearid,monthid,hourid,countyid,roadtypeid,linkid,pollutantid,sourcetypeid,fueltypeid);

FLUSH TABLES;
ALTER TABLE mvs14b_erlt_elp_48141_2020_per_out.idlerate
ADD COLUMN stypemix float,
ADD COLUMN idlerate float,
ADD COLUMN period char(2),
ADD COLUMN Area char(25),
ADD COLUMN VMTmix float,
ADD COLUMN emisfact FLOAT;


UPDATE mvs14b_erlt_elp_48141_2020_per_out.idlerate
SET AREA = "El Paso";
            
FLUSH TABLES;
UPDATE mvs14b_erlt_elp_48141_2020_per_out.idlerate
SET period = "AM" WHERE hourid = 8;

FLUSH TABLES;
UPDATE mvs14b_erlt_elp_48141_2020_per_out.idlerate
SET period = "MD" WHERE hourid = 15;

FLUSH TABLES;
UPDATE mvs14b_erlt_elp_48141_2020_per_out.idlerate
SET period = "PM" WHERE hourid = 18;

FLUSH TABLES;
UPDATE mvs14b_erlt_elp_48141_2020_per_out.idlerate
SET period = "ON" WHERE hourid = 23;

flush tables;
drop table  if exists mvs14b_erlt_elp_48141_2020_per_out.houridlemix;
create table mvs14b_erlt_elp_48141_2020_per_out.houridlemix (SELECT yearid,monthid,hourid,
linkid,sourcetypeid,fueltypeid,sum(activity)as vmx 
FROM mvs14b_erlt_elp_48141_2020_per_out.movesactivityoutput
where activitytypeid = 4 and activity > 0
group by yearid,monthid,hourid,countyid,linkid,sourcetypeid,fueltypeid);

FLUSH TABLES;
UPDATE mvs14b_erlt_elp_48141_2020_per_out.idlerate A
JOIN mvs14b_erlt_elp_48141_2020_per_out.houridlemix B ON
(A.monthid = B.monthid AND
A.hourid = B.hourid AND
A.linkid = B.linkid AND
A.sourcetypeid = B.sourcetypeid AND
A.fueltypeid = B.fueltypeid)
SET A.stypemix = B.vmx;

FLUSH TABLES;
UPDATE mvs14b_erlt_elp_48141_2020_per_out.idlerate
SET idlerate = emission/stypemix ;

flush tables;
drop table  if exists mvs14b_erlt_elp_48141_2020_per_out.SUTmix;
create table mvs14b_erlt_elp_48141_2020_per_out.SUTmix 
SELECT * FROM vmtmix_fy20.todmix 
where TxDOT_Dist = "El Paso" and Daytype = "Weekday" and YearID = 2020 and VMX_RDcode = 5;

flush tables;
UPDATE mvs14b_erlt_elp_48141_2020_per_out.idlerate a
JOIN mvs14b_erlt_elp_48141_2020_per_out.SUTmix  b ON
a.period = b.period AND
a.sourcetypeID = b.MOVES_STcode AND
a.fueltypeID = b.MOVES_FTcode 
SET a.VMTmix = b.VMTmix;

FLUSH TABLES;
UPDATE mvs14b_erlt_elp_48141_2020_per_out.idlerate
SET emisfact = idlerate*VMTmix;

drop table  if exists MVS2014b_ERLT_QAQC.mvs14b_erlt_elp_48141_2020_cer_out_idling_qaqc_from_orignal;
create table MVS2014b_ERLT_QAQC.mvs14b_erlt_elp_48141_2020_cer_out_idling_qaqc_from_orignal
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
FROM mvs14b_erlt_elp_48141_2020_per_out.idlerate
GROUP BY Area,yearid,monthid,hourid,period;