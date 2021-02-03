Emission-Rate-Look-up-Table-TX
==============================

Batch process MOVES output files to get condensed emission rate look-up tables for different districts and years.
Get emission four different processes: running, starts, extended idling, and idling.

## 1. Setup

**Connecting to the database**: The connect_to_server_db function provides the 
ability to connect to different 
databases saved in MariaDB. It takes database name (database_nm), user name (user_nm;
default is "root") as arguments. It looks for the password in the .env file. The 
password is kept in .env file to keep it hidden.

## 2. Analysis Work Flow

## 3. Testing

## 4. Future Improvements



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
