import duckdb
from pathlib import Path

DB_PATH = Path("heatgrid.duckdb")
HEAT_THRESHOLD_C = 32.22

def build_noaa_daily():
    con = duckdb.connect(str(DB_PATH), read_only=False)

    #daily max and mean temp per station
    con.execute(f"""DROP TABLE IF EXISTS noaa_daily_temp;
                """)
    con.execute(f"""CREATE TABLE noaa_daily_temp AS
                SELECT 
                    station,
                    DATE_TRUNC('day', hour_utc) AS day_utc,
                    MAX(temp_C) AS daily_max_temp_C,
                    AVG(temp_C) AS avg_temp_C
                FROM noaa_hourly_avg
                GROUP BY station, day_utc
                ORDER BY station, day_utc;""")
    print("noaa_daily_temp rows: ", con.execute("SELECT COUNT(*) FROM noaa_daily_temp;").fetchone()[0])

    con.close()

#we now have one row per day perstation
def build_noaa_heatwave_flags():
    con = duckdb.connect(str(DB_PATH), read_only=False)

    con.execute("""
        DROP TABLE IF EXISTS noaa_heatwave_flags;
    """)

    con.execute("""
        CREATE TABLE noaa_heatwave_flags AS
        WITH base AS (
            SELECT
                station,
                day_utc,
                daily_max_temp_C,
                avg_temp_C,
                CASE WHEN daily_max_temp_C >= 32.22 THEN 1 ELSE 0 END AS is_hot_day,
                CAST(day_utc AS DATE) AS day_date
            FROM noaa_daily_temp
        ),
                
        hot_days AS (
            SELECT
                station,
                day_date,
                ROW_NUMBER() OVER (PARTITION BY station ORDER BY day_date) AS rn,
                day_date AS thedate

            FROM base
            WHERE is_hot_day = 1
        ),   
        -- Group consecutive hot days:
        -- grp_id = day_date_as_int - rn
        hot_grouped AS (
            SELECT
                station,
                day_date,
                rn,
                (CAST(epoch(day_date) / 86400 AS BIGINT) - rn) AS grp_id
            FROM hot_days
        ),

        -- Find which groups are heatwaves (3+ consecutive hot days)
        heatwave_groups AS (
            SELECT station, grp_id
            FROM hot_grouped
            GROUP BY station, grp_id
            HAVING COUNT(*) >= 3
        ),

        -- Expand heatwave days back to individual dates
        heatwave_days AS (
            SELECT h.station, h.day_date
            FROM hot_grouped h
            JOIN heatwave_groups g
              ON h.station = g.station
             AND h.grp_id = g.grp_id
        )

        SELECT
            b.station,
            b.day_utc,
            b.daily_max_temp_C,
            b.avg_temp_C,
            b.is_hot_day,
            CASE 
                WHEN hd.day_date IS NOT NULL THEN 1
                ELSE 0
            END AS is_heatwave_day
        FROM base b
        LEFT JOIN heatwave_days hd
          ON b.station = hd.station
         AND CAST(b.day_utc AS DATE) = hd.day_date
        ORDER BY b.station, b.day_utc;
    """)

    print("noaa_heatwave_flags rows:",
          con.execute("SELECT COUNT(*) FROM noaa_heatwave_flags;").fetchone()[0])

    #how many hot vs non-hot days?
    print(con.execute("""
        SELECT station, is_hot_day, is_heatwave_day, COUNT(*)
        FROM noaa_heatwave_flags
        GROUP BY station, is_hot_day, is_heatwave_day
        ORDER BY station, is_hot_day, is_heatwave_day;
    """).fetchall())

    con.close()

#finding the total energy consumption per day and the peak hour of day
def build_eia_daily_load():
    con = duckdb.connect(str(DB_PATH), read_only=False)

    con.execute("DROP TABLE IF EXISTS eia_daily_load;")

    # FIX: remove any duplicate region/hour rows before aggregating to days
    con.execute("""
        CREATE TABLE eia_daily_load AS
        WITH hourly_unique AS (
            SELECT DISTINCT
                region,
                hour_utc,
                load_mwh
            FROM eia_load_hourly
        )
        SELECT
            region,
            date_trunc('day', hour_utc) AS day_utc,
            SUM(load_mwh) AS daily_total_mwh,   -- total energy used that day
            MAX(load_mwh) AS daily_peak_mwh     -- highest hourly demand that day
        FROM hourly_unique
        GROUP BY region, day_utc
        ORDER BY region, day_utc;
    """)

    print(
        "eia_daily_load rows:",
        con.execute("SELECT COUNT(*) FROM eia_daily_load;").fetchone()[0]
    )
    con.close()

def heat_load_daily():
    con = duckdb.connect(str(DB_PATH), read_only=False)

    con.execute("DROP TABLE IF EXISTS heat_load_daily;")

    con.execute("""
        CREATE TABLE heat_load_daily AS
        SELECT
            n.station,
            CASE WHEN n.station = 'IAD' THEN 'PJM'
                 WHEN n.station = 'BOS' THEN 'ISNE'
            END AS region,
            n.day_utc,
            n.daily_max_temp_C,
            n.avg_temp_C,
            n.is_hot_day,
            n.is_heatwave_day,
            e.daily_total_mwh,
            e.daily_peak_mwh
        FROM noaa_heatwave_flags n
        LEFT JOIN eia_daily_load e
            ON e.region = CASE WHEN n.station = 'IAD' THEN 'PJM' ELSE 'ISNE' END
           AND e.day_utc = n.day_utc
        ORDER BY region, n.day_utc;
    """)

    print("heat_load_daily rows:",
          con.execute("SELECT COUNT(*) FROM heat_load_daily;").fetchone()[0])

    con.close()

if __name__ == "__main__":
    build_noaa_daily()            # daily NOAA temps
    build_noaa_heatwave_flags()   # hot days + heatwaves
    build_eia_daily_load()        # daily EIA load
    heat_load_daily()             # merge weather + load
