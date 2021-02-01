"""
Interpolate emission rate for intermediate speeds using the MOBILE 6.2 interpolation
formula. It uses inverse of the speed to interpolate. Take the maximum emission rate
between the four sessons as the emission rate for the year.
Created by: Apoorba Bibeka
Date Created: 01/27/2021
"""
from scipy.interpolate import interp1d
import pandas as pd
import numpy as np
import os
from ttierlt.utils import (
    connect_to_server_db,
    get_engine_to_output_to_db,
    PATH_INTERIM_RUNNING,
)

YEAR_LIST = np.arange(2020, 2051, 1)
# Pollutant columns ['CO', 'NOX', 'SO2', 'NO2', 'VOC', 'CO2EQ', 'PM10', 'PM25', 'BENZ',
# 'NAPTH', 'BUTA', 'FORM', 'ACTE',
# 'ACROL', 'ETYB', 'DPM', 'POM']
AVG_SPEED_LIST = [2.5] + list(range(3, 76))
INVERSE_AVG_SPEED_LIST = [1 / avgspeed for avgspeed in AVG_SPEED_LIST]
MAP_INVERSE_AVG_SPD = dict(zip(INVERSE_AVG_SPEED_LIST, AVG_SPEED_LIST))
