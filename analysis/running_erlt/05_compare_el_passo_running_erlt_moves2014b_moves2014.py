"""
Create dashboard for comparing the ERLT output of the recent MOVES run with the results from previous studies.
Created by: Apoorba Bibeka
Date Created: 01/24/2021
"""
import pandas as pd
import os
import ttierlt.utils
from ttierlt.utils import PATH_RAW
import plotly.express as px
import dash
import dash_html_components as html
import dash_core_components as dcc
from dash.dependencies import Input, Output, State
from ttierlt.utils import connect_to_server_db


external_stylesheets = ["https://codepen.io/chriddyp/pen/bWLwgP.css"]
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

conn = connect_to_server_db(database_nm="MVS2014b_ERLT_OUT")
cur = conn.cursor()
erlt_elp_df_2014b = pd.read_sql(
    "SELECT * FROM running_erlt_intermediate_yr_interpolated_spd_interpolated", conn
)
# erlt_elp_df_2014b.to_csv(r"C:\Users\A-Bibeka\ProjectCode\TTI\ERLT_Plot\data\ERLT_elp_with_MOVES_2014b.csv")

erlt_elp_df_2014b_long = (
    erlt_elp_df_2014b.rename(columns={"CO2EQ": "CO2"})
    .melt(
        id_vars=["Area", "yearid", "monthid", "funclass", "avgspeed"],
        value_vars=[
            "CO",
            "NOX",
            "SO2",
            "NO2",
            "VOC",
            "CO2",
            "PM10",
            "PM25",
            "BENZ",
            "NAPTH",
            "BUTA",
            "FORM",
            "ACTE",
            "ACROL",
            "ETYB",
            "DPM",
            "POM",
        ],
        var_name="pollutants",
        value_name="emission_rate_2014b",
    )
    .groupby(["Area", "yearid", "funclass", "avgspeed", "pollutants"])
    .agg(emission_rate_2014b=("emission_rate_2014b", max))
    .reset_index()
)

path_to_2014_erlt = os.path.join(PATH_RAW, "ERLT_with_MOVES_2014.xlsx")
erlt_elp_df_2014 = pd.read_excel(path_to_2014_erlt, sheet_name="El Paso")
erlt_elp_df_2014_long = (
    erlt_elp_df_2014.rename(
        columns={
            "Year": "yearid",
            "Road Description": "rd_desc",
            "Average Speed": "avgspeed",
        }
    )
    .assign(
        funclass=lambda df: df.rd_desc.map(
            {
                "Rural Restricted Access": "Rural-Freeway",
                "Rural Unrestricted Access": "Rural-Arterial",
                "Urban Restricted Access": "Urban-Freeway",
                "Urban Unrestricted Access": "Urban-Arterial",
            }
        ),
    )
    .drop(columns=["Road Type ID", "rd_desc"])
    .melt(
        id_vars=["Area", "yearid", "funclass", "avgspeed"],
        value_vars=[
            "BENZ",
            "NAPTH",
            "BUTA",
            "FORM",
            "ACROL",
            "DPM",
            "POM",
            "NOX",
            "PM10",
            "CO2",
            "PM25",
            "SO2",
            "NO2",
            "VOC",
            "CO",
        ],
        var_name="pollutants",
        value_name="emission_rate_2014",
    )
)

erlt_elp_df_2014_2014b = pd.merge(
    left=erlt_elp_df_2014_long,
    right=erlt_elp_df_2014b_long,
    on=["Area", "yearid", "funclass", "avgspeed", "pollutants"],
).assign(
    yearid_avgspeed=lambda df: df.yearid.astype(str) + "--" + df.avgspeed.astype(str)
)

pollutants_label_value = [
    {"label": pollutant, "value": pollutant}
    for pollutant in erlt_elp_df_2014_2014b.pollutants.unique()
]
avgspeed_values = erlt_elp_df_2014_2014b.avgspeed.unique()
avgspeed_values_marks_dict = {
    int(avg_speed) if avg_speed % 1 == 0 else avg_speed: f"{avg_speed} mph"
    for avg_speed in avgspeed_values
}
min_avgspeed = min(avgspeed_values)
max_avgspeed = max(avgspeed_values)

app.layout = html.Div(
    [
        html.Div(
            className="row",
            children=[
                html.Div(
                    className="eight columns",
                    children=[
                        html.H1(
                            "El Paso Emission Rate Look-Up Table Comparison between MOVES 2014 and MOVES 2014b"
                        ),
                        dcc.Dropdown(
                            id="pollutant-dropdown",
                            options=pollutants_label_value,
                            value="CO2",
                        ),
                        dcc.Graph(id="erlt_com_scatter"),
                        dcc.Slider(
                            id="avg_speed-slider",
                            min=min_avgspeed,
                            max=max_avgspeed,
                            value=min_avgspeed,
                            step=None,
                            marks=avgspeed_values_marks_dict,
                        ),
                    ],
                ),
                html.Div(className="four columns"),
            ],
        ),
    ]
)


@app.callback(
    Output("erlt_com_scatter", "figure"),
    [Input("pollutant-dropdown", "value")],
    [State("avg_speed-slider", "value")],
)
def update_bar_chart(pollutant_val, avgspeed_val):
    erlt_elp_df_2014_2014b_fil = erlt_elp_df_2014_2014b.loc[
        lambda df: (df.pollutants == pollutant_val) & (df.avgspeed == avgspeed_val)
    ].assign(Year=lambda df: df.yearid.astype(str))
    fig = px.scatter(
        data_frame=erlt_elp_df_2014_2014b_fil,
        x="emission_rate_2014",
        y="emission_rate_2014b",
        symbol="Year",
        color="Year",
        facet_col="funclass",
        facet_col_wrap=2,
    )
    return fig


if __name__ == "__main__":
    app.run_server(debug=True, port=4050)
