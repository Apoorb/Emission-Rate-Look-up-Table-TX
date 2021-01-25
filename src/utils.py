from pathlib import Path
import os

def get_project_root() -> Path:
    return Path(__file__).parent.parent

# GLOBAL PATHS
MAP_COUNTY_ABB_FULL_NM = {"elp": "El Paso"}
PATH_TO_MARIA_DB_DATA = "C:/ProgramData/MariaDB/MariaDB 10.4/data"
PATH_TO_PROJECT_ROOT = get_project_root()
PATH_INTERIM = os.path.join(PATH_TO_PROJECT_ROOT, "data", "interim")
PATH_RAW = os.path.join(PATH_TO_PROJECT_ROOT, "data", "raw")
PATH_TO_ERLT_FILES = os.path.join(PATH_INTERIM, "ERLT Files")
if not os.path.exists(PATH_TO_ERLT_FILES):
    os.mkdir(PATH_TO_ERLT_FILES)
TEMPLATE_DB_NM = "mvs14b_erlt_elp_48141_2020_1_cer_out"  # Database used for developing the 1st set of SQL queries. It's
# name would be replaced by other database name as we iterate over the different databases.