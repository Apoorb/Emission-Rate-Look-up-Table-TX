"""
Batch copy files from one-drive to the database folder.
"""

import os
from functools import reduce
import functools
import operator
import re
from shutil import copytree
from ttierlt.movesdb import MovesDb
from ttierlt.utils import connect_to_server_db

if __name__ == "__main__":
    mariadb_data_dir = r"C:\ProgramData\MariaDB\MariaDB 10.4\data"

    path_to_parent_dir = (
        r"C:\Users\A-Bibeka\Texas A&M Transportation Institute\HMP - "
        r"HMP Active Projects\MOVES_ERLT_2019\rates"
    )
    district_abbs = ["elp", "aus", "bmt", "crp", "dal", "ftw", "hou", "wac", "sat"]
    district_abbs = ["elp", "aus", "bmt", "crp", "dal", "ftw"]

    district_abb = "elp"

    county_files_pat = MovesDb.county_level_db.pattern
    # 'mvs14b_erlt_\\S{3}_\\d{5}_20\\d{2}_\\d{1,2}_cer_out'
    project_files_pat = MovesDb.project_level_db.pattern
    # 'mvs14b_erlt_\\S{3}_\\d{5}_20\\d{2}_per_out'

    # Get list of already copied databases.
    conn = connect_to_server_db("")
    cur = conn.cursor()
    cur.execute("SHOW DATABASES;")
    already_copied_db = cur.fetchall()
    already_copied_db = functools.reduce(operator.iconcat, already_copied_db, [])
    conn.close()

    county_db_list = []
    for district_abb in district_abbs:
        spec_county_files_regex = re.compile(
            county_files_pat.replace("\\S{3}", district_abb)
        )
        subfolders_county = [district_abb, "MOVESrunfiles", "county", "MOVESOutput"]
        path_to_county_files = reduce(
            os.path.join, [path_to_parent_dir] + subfolders_county
        )
        list_of_dirs = os.listdir(path_to_county_files)
        list_of_county_dir = [
            os.path.join(path_to_county_files, file)
            for file in list_of_dirs
            if re.match(spec_county_files_regex, file)
        ]
        is_num_file_eq_16 = len(list_of_county_dir) == 64
        assert is_num_file_eq_16, (
            "Check why the file count is not 64; 16 years * 4 " "months"
        )
        county_db_list.append(list_of_county_dir)
    county_db_list = functools.reduce(operator.iconcat, county_db_list, [])
    assert len(county_db_list) == 64 * len(district_abbs), (
        "There should be 64 " "databases in each of the" " 9 districts."
    )

    project_db_path_list = []
    for district_abb in district_abbs:
        spec_project_files_regex = re.compile(
            project_files_pat.replace("\\S{3}", district_abb)
        )
        subfolders_project = [district_abb, "MOVESrunfiles", "project", "MOVESOutput"]
        path_to_project_files = reduce(
            os.path.join, [path_to_parent_dir] + subfolders_project
        )
        list_of_dirs = os.listdir(path_to_project_files)
        list_of_prj_dir = [
            os.path.join(path_to_project_files, file)
            for file in list_of_dirs
            if re.match(spec_project_files_regex, file)
        ]
        is_num_file_eq_16 = len(list_of_prj_dir) == 16
        assert is_num_file_eq_16, "Check why the file count is not 16; 16 years"
        project_db_path_list.append(list_of_prj_dir)

        test_loc = (
            r"C:\Users\A-Bibeka\OneDrive - Texas A&M Transportation "
            r"Institute\Documents\Projects\ERLT\data\raw"
        )

    project_db_path_list = functools.reduce(operator.iconcat, project_db_path_list, [])
    assert len(project_db_path_list) == 16 * len(district_abbs), (
        "There should be 16" " databases in  " "each of the 9 " "districts."
    )

    for db_path in county_db_list:
        db_nm = os.path.basename(db_path)
        if db_nm in already_copied_db:
            continue
        print(f"Copying {db_nm}")
        copytree(
            src=db_path, dst=os.path.join(mariadb_data_dir, db_nm), dirs_exist_ok=True
        )

    for db_path in project_db_path_list:
        if db_nm in already_copied_db:
            continue
        db_nm = os.path.basename(db_path)
        print(f"Copying {db_nm}")
        copytree(
            src=db_path, dst=os.path.join(mariadb_data_dir, db_nm), dirs_exist_ok=True
        )
