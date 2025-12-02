from prefect import task, flow
import duckdb

import os
import logging 
import time 
from datetime import datetime
from pathlib import Path
import pandas as pd 

# import functions for task
from scripts.noaa_ingest import load_noaa_hourly, build_noaa_hourly_avg
from scripts.eia_ingest import fetch_eia_data, load_eia_to_duckdb
from scripts.base_combine_processing import combine_table

DATA_DIR = Path("data")
DB_PATH = Path("heatgrid.duckdb")

# NOAA Configs
STATIONS = {
    "IAD": "72403093738",   # Washington dulles
    "BOS": "72509014739",   # Boston 
    "NYC": "74486094789",   # NYC 
    "LAX": "72295023174",   # LAX

}
YEARS = [2019, 2020, 2021, 2022, 2023, 2024, 2025]

NOAA_BASE_URL = "https://www.ncei.noaa.gov/data/global-hourly/access/{year}/{station}.csv"

DOWNLOAD_DELAY = 1.0

RAW_TABLE = "noaa_hourly_raw"
TEMP_TABLE = "noaa_hourly_temp"
AVG_TABLE  = "noaa_hourly_avg"

# EIA Configs
EIA_RAW_TABLE = "eia_hourly_raw"
EIA_HOURLY_TABLE = "eia_load_hourly"

PARENT_SUBBAS = {
    "PJM":  ["DOM"],            # DC / NOVA
    "ISNE": ["4007", "4008"],   # Western/Central MA + Northeast MA
    "NYIS": ["ZONJ", "ZONK"],   # NYC + Long Island
    "CISO": ["SCE"],            # Southern California Edison (LA area)
}

START = "2019-01-01T00"
END   = "2025-08-27T04"

EIA_BASE_URL = "https://api.eia.gov/v2/electricity/rto/region-sub-ba-data/data/"

EIA_API_KEY = os.getenv("EIA_API_KEY")


# Tasks
@task
def load_noaa():
    load_noaa_hourly()

@task
def build_noaa():
    build_noaa_hourly_avg() 

@task
def eia_fetch():
    fetch_eia_data(parent_subbas=PARENT_SUBBAS, start=START, end=END)

@task
def build_eia():
    load_eia_to_duckdb()

@task
def build_combine():
    combine_table() 

# Flow
@flow(name="heatgrid_ingest_processing")
def heatgrid_flow():
    load_noaa()
    build_noaa()
    eia_fetch()
    build_eia()
    build_combine()

if __name__ == "__main__":
    heatgrid_flow()