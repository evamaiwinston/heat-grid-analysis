import duckdb
import requests

import logging
import time
from pathlib import Path
from datetime import datetime

# Configs
STATIONS = {
    "IAD": "72403093738",   # Washington dulles
    "BOS": "72509014739",   # Boston 
    "ATL": "72219013874",   # Atlanta 
    "NYC": "74486094789",   # NYC 
    "DFW": "72259003927",   # Dallas–Fort Worth
    "LAX": "72295023174",   # LAX
    "SEA": "72793024233",   # Seattle-Tacoma

}
YEARS = [2019, 2020, 2021, 2022, 2023, 2024, 2025]

BASE_URL = "https://www.ncei.noaa.gov/data/global-hourly/access/{year}/{station}.csv"

DATA_DIR = Path("data")
DB_PATH = Path("heatgrid.duckdb")

DOWNLOAD_DELAY = 1.0

RAW_TABLE = "noaa_hourly_raw"
TEMP_TABLE = "noaa_hourly_temp"
AVG_TABLE  = "noaa_hourly_avg"


# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="logs/noaa_ingest.log",
)
logger = logging.getLogger(__name__)
logger.info("--------- New NOAA run ---------")

def load_noaa_hourly():
    con = None
    try:
        DATA_DIR.mkdir(parents=True, exist_ok=True)

        # Download CSV file
        print("Downloading NOAA data...") 
        for code, station_id in STATIONS.items():
            for year in YEARS:
                url = BASE_URL.format(year=year, station=station_id)
                out = DATA_DIR / f"{code}_{year}.csv"

                if out.exists():
                    logger.info(f"[download] exists, skip {out.name}")
                    continue

                logger.info(f"[download] GET {url} -> {out.name}")
                try:
                    with requests.get(url, stream=True, timeout=(10, 60)) as r:
                        r.raise_for_status()
                        with open(out, "wb") as f:
                            for chunk in r.iter_content(chunk_size=1 << 20):
                                if chunk:
                                    f.write(chunk)
                    time.sleep(DOWNLOAD_DELAY)
                except Exception as e:
                    logger.error(f"[download] failed for {out.name}: {e}")

        
        # Ingest into DuckDB
        con = duckdb.connect(str(DB_PATH), read_only=False)
        logger.info("Connected to DuckDB instance")


        con.execute(f"""
                    DROP TABLE IF EXISTS {RAW_TABLE};
         """)
        logger.info("Dropped raw table if exists")

        con.execute(f"""
                    DROP TABLE IF EXISTS {TEMP_TABLE};
        """)
        logger.info("Dropped temp table if exists")

        # Create tables from CSVs
        print("Creating tables...")
        con.execute(f"""
            CREATE TABLE {RAW_TABLE} AS
            SELECT *
            FROM read_csv_auto('{DATA_DIR}/*.csv', header = TRUE,
            union_by_name = TRUE, files_to_sniff = -1);
        """)
        

        # Format temps for temp table
        con.execute(f"""
            CREATE TABLE {TEMP_TABLE} AS
            SELECT
            -- Map numeric NOAA station codes to readable labels
            CASE 
                WHEN station = '72403093738' THEN 'IAD'   -- Dulles
                WHEN station = '72509014739' THEN 'BOS'   -- Boston 
                WHEN station = '72219013874' THEN 'ATL'   -- Atlanta
                WHEN station = '74486094789' THEN 'NYC'   -- New York City 
                WHEN station = '72259003927' THEN 'DFW'   -- Dallas-Fort Worth
                WHEN station = '72295023174' THEN 'LAX'   -- Los Angeles
                WHEN station = '72793024233' THEN 'SEA'   -- Seattle 
                ELSE CAST(STATION AS VARCHAR)
            END AS station,

            -- Timestamp in UTC
            DATE::TIMESTAMP AS ts_utc,

            -- Cleaned temperature in Celsius
            CASE 
                WHEN split_part(TMP, ',', 1) = '+9999' THEN NULL
                ELSE TRY_CAST(split_part(TMP, ',', 1) AS INTEGER) / 10.0
            END AS temp_C

            FROM {RAW_TABLE};
        """)

        # Counts
        raw_count = con.execute(f"SELECT COUNT(*) FROM {RAW_TABLE};").fetchone()[0]
        temp_count = con.execute(f"SELECT COUNT(*) FROM {TEMP_TABLE};").fetchone()[0]

        print("\nNOAA ROW COUNTS")
        print(f"{RAW_TABLE}:  {raw_count}")
        print(f"{TEMP_TABLE}: {temp_count}")


    except Exception as e:
        print(f"NOAA load error: {e}")
        logger.error(f"NOAA load error: {e}")
    
    # Close DuckDB connection 
    finally:
        if con is not None:
            con.close()
            logger.info("Closed DuckDB (NOAA)")

def build_noaa_hourly_avg():
    con = None
    try:
        con = duckdb.connect(str(DB_PATH), read_only=False)
        logger.info("Connected to DuckDB for hourly avg build")

        # Drop old table if it exists
        con.execute(f"DROP TABLE IF EXISTS {AVG_TABLE};")
        logger.info(f"Dropped {AVG_TABLE} if it existed")

        logger.info(f"Creating {AVG_TABLE} from {TEMP_TABLE}")
        con.execute(f"""
            CREATE TABLE {AVG_TABLE} AS
            SELECT
                station,
                date_trunc('hour', ts_utc) AS hour_utc,
                AVG(temp_C) AS temp_C
            FROM {TEMP_TABLE}
            GROUP BY station, hour_utc
            ORDER BY station, hour_utc;
        """)

        avg_count = con.execute(f"SELECT COUNT(*) FROM {AVG_TABLE};").fetchone()[0]
        logger.info(f"[counts] {AVG_TABLE}: {avg_count}")
        print(f"{AVG_TABLE}: {avg_count} rows")

    except Exception as e:
        print(f"NOAA hourly avg error: {e}")
        logger.error(f"NOAA hourly avg error: {e}")
    finally:
        if con is not None:
            con.close()
            logger.info("Closed DuckDB (NOAA hourly avg)")

if __name__ == "__main__":
    load_noaa_hourly()
    build_noaa_hourly_avg()

