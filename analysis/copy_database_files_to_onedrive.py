"""
Batch copy files to one-drive from the database folder.
"""

import os
from functools import reduce
import functools
import operator
import re
import shutil
from ttierlt_v1.movesdb import MovesDb
from pathlib import Path
from ttierlt_v1.utils import connect_to_server_db

if __name__ == "__main__":
    mariadb_data_dir = r"C:\ProgramData\MariaDB\MariaDB 10.4\data"

    path_to_out_dir = r"C:\Users\a-bibeka\Documents"
    district_abbs = ["elp", "aus", "bmt", "crp", "dal", "ftw", "hou", "wac", "sat"]

    county_files_pat = MovesDb.county_level_db.pattern
    # 'mvs14b_erlt_\\S{3}_\\d{5}_20\\d{2}_\\d{1,2}_cer_out'
    project_files_pat = MovesDb.project_level_db.pattern
    # 'mvs14b_erlt_\\S{3}_\\d{5}_20\\d{2}_per_out'

    for district_abb in district_abbs:
        spec_county_files_regex = re.compile(
            county_files_pat.replace("\\S{3}", district_abb)
        )
        list_of_dbs = os.listdir(mariadb_data_dir)
        list_of_county_db = [
            os.path.join(mariadb_data_dir, file)
            for file in list_of_dbs
            if re.match(spec_county_files_regex, file)
        ]
        subfolders_county = [district_abb, "county"]
        path_to_out_county_files = reduce(
            os.path.join, [path_to_out_dir] + subfolders_county
        )
        Path(path_to_out_county_files).mkdir(parents=True, exist_ok=True)

        for db_path in list_of_county_db:
            db_path
            shutil.move(db_path, path_to_out_county_files)

    for district_abb in district_abbs:
        spec_county_files_regex = re.compile(
            county_files_pat.replace("\\S{3}", district_abb)
        )
        subfolders_county = [district_abb, "county"]
        path_to_out_county_files = reduce(
            os.path.join, [path_to_out_dir] + subfolders_county
        )
        list_of_county_db = [
            file
            for file in os.listdir(path_to_out_county_files)
            if re.match(spec_county_files_regex, file)
        ]

        is_num_file_eq_64 = len(list_of_county_db) == 64
        assert is_num_file_eq_64, (
            "Check why the file count is not 64; 16 years * 4 " "months"
        )

    for district_abb in district_abbs:
        spec_project_files_regex = re.compile(
            project_files_pat.replace("\\S{3}", district_abb)
        )
        list_of_dbs = os.listdir(mariadb_data_dir)
        list_of_project_db = [
            os.path.join(mariadb_data_dir, file)
            for file in list_of_dbs
            if re.match(spec_project_files_regex, file)
        ]
        subfolders_project = [district_abb, "project"]
        path_to_out_project_files = reduce(
            os.path.join, [path_to_out_dir] + subfolders_project
        )
        Path(path_to_out_project_files).mkdir(parents=True, exist_ok=True)

        for db_path in list_of_project_db:
            db_path
            shutil.move(db_path, path_to_out_project_files)

    for district_abb in district_abbs:
        spec_project_files_regex = re.compile(
            project_files_pat.replace("\\S{3}", district_abb)
        )
        subfolders_project = [district_abb, "project"]
        path_to_out_project_files = reduce(
            os.path.join, [path_to_out_dir] + subfolders_project
        )
        list_of_project_db = [
            file
            for file in os.listdir(path_to_out_project_files)
            if re.match(spec_project_files_regex, file)
        ]

        is_num_file_eq_16 = len(list_of_project_db) == 16
        assert is_num_file_eq_64, (
            "Check why the file count is not 64; 16 years * 4 " "months"
        )
