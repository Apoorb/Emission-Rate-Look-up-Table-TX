FLUSH TABLES;
Update mvs14b_erlt_elp_48141_2022_7_cer_out.emisrate
SET txledfac = 1;

flush tables;
UPDATE mvs14b_erlt_elp_48141_2022_7_cer_out.emisrate
SET emisFact = ERate*stypemix*HourMix*txledfac;

FLUSH TABLES;
Update mvs14b_erlt_elp_48141_2048_10_cer_out.emisrate
SET txledfac = 1;

flush tables;
UPDATE mvs14b_erlt_elp_48141_2048_10_cer_out.emisrate
SET emisFact = ERate*stypemix*HourMix*txledfac;