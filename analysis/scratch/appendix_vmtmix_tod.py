"""
Output VMT Mix by source type and fuel type.
"""

import pandas as pd
import os
from ttierlt.utils import connect_to_server_db

out_path = (
    r"C:\Users\a-bibeka\Texas A&M Transportation Institute"
    r"\HMP - Projects - Documents\ERLT_Development_2014b\Report"
    r"\erlt_appendix_h.xlsx"
)
conn = connect_to_server_db(database_nm="vmtmix_fy20")
cur = conn.cursor()

DISTRICTS_ALL = (
    "El Paso",
    "Austin",
    "Corpus Christi",
    "Beaumont",
    "Dallas",
    "Fort Worth",
    "Houston",
    "Waco",
    "San Antonio",
)

vmtmix_tod = pd.read_sql(
    f"""SELECT * FROM todmix 
    WHERE TxDOT_Dist IN {DISTRICTS_ALL}; """,
    conn,
)

moves_rdtypeid_map = {
    1: "Off-Network",
    2: "Rural Restricted Access",
    3: "Rural Unrestricted Access",
    4: "Urban Restricted Access",
    5: "Urban Unrestricted Access",
}

vmtmix_tod_fil = (
    vmtmix_tod.loc[
        lambda df: (df.YearID.isin(range(2020, 2055, 5))) & (df.Daytype == "Weekday")
    ]
    .assign(
        Period=lambda df: pd.Categorical(
            values=df.Period, categories=["AM", "MD", "PM", "ON"], ordered=True
        ),
        VMX_RDcode=lambda df: df.VMX_RDcode.astype(int),
        road_desc=lambda df: df.VMX_RDcode.map(moves_rdtypeid_map),
    )
    .sort_values(
        by=[
            "TxDOT_Dist",
            "YearID",
            "Period",
            "VMX_RDdesc",
            "MOVES_STcode",
            "MOVES_FTcode",
        ]
    )
    .rename(
        columns={
            "TxDOT_Dist": "District",
            "YearID": "Year",
            "road_desc": "Roadway Type",
            "MOVES_STdesc": "SUT",
            "MOVES_FTdesc": "Fuel Type",
            "VMTmix": "VMT Factor",
        }
    )
    .filter(
        items=[
            "District",
            "Year",
            "Period",
            "Roadway Type",
            "SUT",
            "Fuel Type",
            "VMT Factor",
        ]
    )
)


vmtmix_tod_sum = (
    vmtmix_tod_fil.assign(Period=lambda df: df.Period.astype(str))
    .groupby(
        [
            "District",
            "Year",
            "Period",
            "Roadway Type",
            "SUT",
            "Fuel Type",
        ]
    )
    .agg(count1=("VMT Factor", "count"))
    .reset_index()
)


assert vmtmix_tod_sum.count1.max() == 1
assert vmtmix_tod_sum.count1.min() == 1

writer = pd.ExcelWriter(out_path, engine="xlsxwriter")
vmtmix_tod_fil.to_excel(
    writer, startcol=0, startrow=3, sheet_name="Sheet1", index=False
)
worksheet = writer.sheets["Sheet1"]
worksheet.write_string(0, 0, "Appendix H: Time of Day VMT Mixes")
writer.save()
