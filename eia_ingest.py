import os
import logging
from pathlib import Path

import requests
import duckdb
import pandas as pd

# Config
DB_PATH = Path("heatgrid.duckdb")

EIA_RAW_TABLE = "eia_hourly_raw"
EIA_HOURLY_TABLE = "eia_load_hourly"

PARENTS = ["PJM", "ISNE"] # Dc area and New england 

START = "2019-01-01T00"
END   = "2024-12-31T23"

BASE_URL = "https://api.eia.gov/v2/electricity/rto/region-sub-ba-data/data/"

EIA_API_KEY = os.getenv("EIA_API_KEY")


# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="logs/eia_ingest.log",
)
logger = logging.getLogger(__name__)
logger.info("--------- New EIA run ---------")


# Fetch from API
def fetch_eia_data(parents, start, end):
    if not EIA_API_KEY:
        raise RuntimeError("EIA_API_KEY is not set in the environment")
    
    all_rows = []
    offset = 0
    page_size = 5000

    logger.info(f"[EIA] Fetching parents={parents} from {start} to {end}")
    print(f"Fetching EIA data for parents={parents} ...")

    while True:
        params = {
            "api_key": EIA_API_KEY,
            "frequency": "hourly",
            "data[0]": "value",
            "start": start,
            "end": end,
            "sort[0][column]": "period",
            "sort[0][direction]": "asc",
            "offset": offset,
            "length": page_size,
        }

        params["facets[parent][]"] = parents


        logger.info(f"[EIA] Request offset={offset}")
        if offset % 100000 == 0:
            print(f"[EIA] Progress: offset={offset} rows fetched")
        r = requests.get(BASE_URL, params=params, timeout=30)
        r.raise_for_status()
        payload = r.json()

        rows = payload.get("response", {}).get("data", [])
        if not rows:
            logger.info("[EIA] No more rows, stopping pagination")
            break

        all_rows.extend(rows)
        logger.info(f"[EIA] Retrieved {len(rows)} rows (total so far: {len(all_rows)})")

        if len(rows) < page_size:
            # last page
            break

        offset += page_size

    logger.info(f"[EIA] Total rows fetched: {len(all_rows)}")
    return all_rows

# Ingest into DuckDB
def load_eia_to_duckdb():
    con = None
    try:

        rows = fetch_eia_data(PARENTS, START, END)
        if not rows:
            print("No EIA rows returned. Check API key and params")
            return

        df = pd.DataFrame(rows)

        con = duckdb.connect(str(DB_PATH), read_only=False)
        logger.info("Connected to DuckDB instance")

        ##print("EIA df columns:", df.columns.tolist())

        expected_cols = {"period", "parent", "subba", "value"}
        missing = expected_cols - set(df.columns)
        if missing:
            logger.error(f"[EIA] Missing expected columns in response: {missing}")
            raise RuntimeError(f"EIA response missing columns: {missing}")

        # Raw table
        con.execute(f"DROP TABLE IF EXISTS {EIA_RAW_TABLE};")
        logger.info(f"Dropped {EIA_RAW_TABLE} if existed")

        con.register("eia_df", df)
        logger.info("Registered eia_df in DuckDB")
        print("Creating tables...")

        con.execute(f"""
            CREATE TABLE {EIA_RAW_TABLE} AS
            SELECT
                period,
                parent,
                "parent-name" AS parent_name,
                subba,
                "subba-name" AS subba_name,
                CAST(value AS DOUBLE) AS value_mwh
            FROM eia_df;
        """)
        raw_count = con.execute(
            f"SELECT COUNT(*) FROM {EIA_RAW_TABLE};"
        ).fetchone()[0]
        logger.info(f"[EIA] {EIA_RAW_TABLE}: {raw_count} rows")

        # Aggregated hourly load per parent region
        con.execute(f"DROP TABLE IF EXISTS {EIA_HOURLY_TABLE};")
        logger.info(f"Dropped {EIA_HOURLY_TABLE} if existed")

        con.execute(f"""
            CREATE TABLE {EIA_HOURLY_TABLE} AS
            SELECT
                parent AS region,
                STRPTIME(period, '%Y-%m-%dT%H') AS hour_utc,
                SUM(value_mwh) AS load_mwh
            FROM {EIA_RAW_TABLE}
            GROUP BY 1, 2
            ORDER BY 1, 2;
        """)
        hourly_count = con.execute(
            f"SELECT COUNT(*) FROM {EIA_HOURLY_TABLE};"
        ).fetchone()[0]
        logger.info(f"[EIA] {EIA_HOURLY_TABLE}: {hourly_count} rows")

        print("\nEIA TABLE COUNTS")
        print(f"{EIA_RAW_TABLE}:     {raw_count}")
        print(f"{EIA_HOURLY_TABLE}: {hourly_count}")

        sample = con.execute(f"""
            SELECT *
            FROM {EIA_HOURLY_TABLE}
            ORDER BY region, hour_utc
            LIMIT 10;
        """).fetchall()

        print("\nSample hourly load rows:")
        for row in sample:
            print(row)

    except Exception as e:
        print(f"EIA load error: {e}")
        logger.error(f"EIA load error: {e}")

    finally:
        if con is not None:
            con.close()
            logger.info("Closed DuckDB")


if __name__ == "__main__":
    load_eia_to_duckdb()