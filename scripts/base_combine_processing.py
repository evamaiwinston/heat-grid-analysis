import duckdb

def combine_table(db_path: str = "heatgrid.duckdb") -> None:

    # connect to DuckDB
    try:
        con = duckdb.connect(db_path)
    except Exception as e:
        print(f"Error connecting to DuckDB at {db_path}: {e}")
        return

    
    # Drop table if it exists
    con.execute("DROP TABLE IF EXISTS base_combine;")

    # Create temp mapping table
    con.execute("""
    CREATE TEMP TABLE mapping AS
    SELECT * FROM (VALUES
        ('IAD', 'PJM'),
        ('BOS', 'ISNE'),
        ('NYC', 'NYIS'),
        ('LAX', 'CISO')
    ) AS m(station, region);
    """)
    
    # Combine tables
    con.execute("""
    CREATE TABLE base_combine AS
    SELECT
        n.hour_utc       AS hour_utc,
        n.station        AS location,
        n.temp_c         AS temp,
        e.load_mwh       AS load_mwh
    FROM noaa_hourly_avg n
    JOIN mapping m
      ON n.station = m.station
    JOIN eia_load_hourly e
      ON e.region = m.region
     AND e.hour_utc = n.hour_utc
    ORDER BY hour_utc, location;
    """)

    # cleaning- Remove impossible values
    con.execute("""
        DELETE FROM base_combine
        WHERE load_mwh > 500000 OR load_mwh <= 0;
    """)
    
    # close db
    con.close()
    print("base_combine table created.")

