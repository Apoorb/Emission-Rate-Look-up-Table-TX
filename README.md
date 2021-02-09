Emission-Rate-Look-up-Table-TX
==============================

The Emission-Rate-Look-up-Table-TX repository contains the `ttierlt` Python (>=3.9) 
package for batch processing MOVES output files to get composite emission rate look-up 
tables for different Texas districts and years. This module can provide mission 
rates for four different processes: running, starts, extended idling, and idling. 

Code for emission rate estimation exists in multiple form:
1. Code usable for any MOVES 2014 county and project data. The Python package `ttierlt`
contains code that can be used to get rates for four different processes: running, 
   starts, extended idling, and idling from a MOVES output. 
2. Code for batch processing multiple MOVES output files. The code in the folder 
   analysis folder can be used to batch process multiple MOVES output databases.

## 1. Setup
1. Create a Python environment based on the included requirements.txt file. 
   Instructions on doing so can be found [here](https://stackoverflow.
   com/questions/48787250/set-up-virtualenv-using-a-requirements-txt-generated-by
   -conda).  
   **Note 1 SQL based processing**: This module uses MariaDB 10.4 to conduct the data 
   processing. 
   MariaDB and sqlalchemy library provides the connecting mechanism.  
   **Note 2**:**Connecting to the database**: The connect_to_server_db function provides the ability to connect to different 
databases saved in MariaDB. It takes database name (database_nm), user name (user_nm; default is "root") as arguments. It looks for the password in the .env file. The 
password is kept in .env file to keep it hidden. The default password saved in .env 
   is for my (Apoorba's MariaDB database); change the password to whatever is your 
   MariaDB password.
2. Copy the MOVES county and project level databases to the MariaDB data folder. 
   For me (Apoorba), it is saved [C:\ProgramData\MariaDB\MariaDB 10.4\data]
   (C:\ProgramData\MariaDB\MariaDB 10.4\data).  
    **Note 1: Database formats**: This module uses the information from the database 
name, thus the code is this module is depended on consistent naming of the MOVES 
county and project output databases. 


        Following are county and project level database naming convention:
            mvs14b_erlt_{district_abb}_*_20[0-9][0-9]_[0-9]*_cer_out and 
            mvs14b_erlt_{district_abb}_*_20[0-9][0-9]_per_out. 
    
        County level database name example: mvs14b_erlt_elp_48141_2020_1_cer_out can be 
        decomposed as follows:
            mvs14b: MOVES 2014---index 0
            erlt: Project name; emission rate look-up table---index 1
            elp: El-Passo---index 2
            48141: FIPS code for El-Passo County---index 3
            2020: Year---index 4
            1: Month; Jan---index 5
            cer: County level run
            out: Output database
    
        Project level database name example: mvs14b_erlt_elp_48141_2020_per_out can be 
        decomposed as follows:
            mvs14b: MOVES 2014---index 0
            erlt: Project name; emission rate look-up table---index 1
            elp: El-Passo---index 2
            48141: FIPS code for El-Passo County---index 3
            2020: Year---index 4
            per: Project level run 
            out: Output database

3. Copy the helper VMT and hourly mixes from data/iterim/helper_schema to the 
   MariaDB data folder. 
4. Runs the scripts in order in the analysis/running, analysis/idling, 
   analysis/starts/ and analysis/extnidle folders.  
   **Note**: These scripts are only tested on PyCharm with 
   Emission-Rate-Look-up-Table-TX as the project folder. Some of the folder paths 
   defined within the module might not work if individual scripts are run directly 
   or if project folder is not set to Emission-Rate-Look-up-Table-TX. 
   
## 2. Analysis Work Flow

A sample workflow consists of the following steps:

1. 00_create_txled_database.py: Create TxLED database from the TxLED data saved 
   in data/raw/ERLT-TxLED Factor Summary.xlsx. 
2. running_erlt/00_setting_up_schemas.py: Create mvs2014b_erlt_out, 
   mvs2014b_erlt_qaqc, and mvs2014b_erlt_conflicted schemas for storing outputs
3. running_erlt/01_batch_run_running.py: Batch run the `ttierlt.running.
   running_batch_sql`module on different MOVES output databases. This script outputs 
   running_erlt_intermediate table. 
4. running_erlt/02_interpolate_emission_rate_between_yr_spd_agg_month.py: 
   Interpolate emission rates for intermediate years and speeds. This script outputs 
   running_erlt_intermediate_yr_interpolated 
   and running_erlt_intermediate_yr_spd_interpolated_no_monthid table.
5. running_erlt/running_sql: Create running composite emission rate tables for 
   individual MOVES output database. These tables are saved in mvs2014b_erlt_qaqc 
   and are used for testing. An example of QAWC output table would be 
   mvs14b_erlt_aus_48453_2020_1_cer_out_running_qaqc_from_orignal; this is the QAQC 
   output for Austin, 2020, January. 
6. tests/test_running_erlt_df: Run tests on running composite emission rate tables. 
   One of the tests in this module compares the output from step 4 with step 5. 
   Majority of the tests are checking if all the expected groups are present in the 
   final tables and the values within these groups and across these groups varies 
   indicating that we did not use the same input for multiple scenarios by mistake.

## 3. Caveats
**to_sql** will sometimes throw operational error when the ouptut database is large 
(e.g. 70,000 rows). Add a chunksize parameter with chunksize = 10,000 to make it work.

## 4 Future Improvements
1. Address all the FixMes and TODOs in the project folder.


Project Organization
------------

    ├── LICENSE
    ├── README.md          <- The top-level README for developers using this project.
    ├── data
    │   ├── external       <- Data from third party sources.
    │   ├── interim        <- Intermediate data that has been transformed.
    │   ├── processed      <- The final, canonical data sets for modeling.
    │   └── raw            <- The original, immutable data dump.
    │
    ├── docs               <- A default Sphinx project; see sphinx-doc.org for details
    │
    ├── notebooks          <- Jupyter notebooks. Naming convention is a number (for ordering),
    │                         the creator's initials, and a short `-` delimited description, e.g.
    │                         `1.0-jqp-initial-data-exploration`.
    │
    ├── references         <- Data dictionaries, manuals, and all other explanatory materials.
    │
    ├── reports            <- Generated analysis as HTML, PDF, LaTeX, etc.
    │   └── figures        <- Generated graphics and figures to be used in reporting
    │
    ├── requirements.txt   <- The requirements file for reproducing the analysis environment, e.g.
    │                         generated with `pip freeze > requirements.txt`
    │
    ├── setup.py           <- makes project pip installable (pip install -e .) so src can be imported
    │
    ├── ttierlt                <- Source code for use in this project.
    │   ├── __init__.py    <- Makes ttierlt a Python module
    │   │
    │   └── running           <- Packages to process running emissions.
    │
    ├── analysis               <- Scripts to process the emission rate data.
    │   │
    │   └── running           <- Scripts to process running emissions.
    │
    └── tox.ini            <- tox file with settings for running tox; see tox.readthedocs.io


--------

<p><small>Project based on the <a target="_blank" href="https://drivendata.github.io/cookiecutter-data-science/">cookiecutter data science project template</a>. #cookiecutterdatascience</small></p>
