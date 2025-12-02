import duckdb

DB_PATH = "heatgrid.duckdb"

con = duckdb.connect(DB_PATH)

con.execute("DROP TABLE IF EXISTS base_combine;")


con.execute("""
CREATE TEMP TABLE mapping AS
SELECT * FROM (VALUES
    ('IAD', 'PJM'),
    ('BOS', 'ISNE'),
    ('NYC', 'NYIS'),
    ('LAX', 'CISO')
) AS m(station, region);
""")


con.execute("""
CREATE TABLE base_combine AS
SELECT
    n.hour_utc       AS hour_utc,
    n.station        AS location,
    n.temp_c         AS temp,        -- <-- change if needed
    e.load_mwh       AS load_mwh
FROM noaa_hourly_avg n
JOIN mapping m
  ON n.station = m.station
JOIN eia_load_hourly e
  ON e.region = m.region
 AND e.hour_utc = n.hour_utc
ORDER BY hour_utc, location;
""")

# Clean- remove impossible load values
con.execute("""
    DELETE FROM base_combine
    WHERE load_mwh > 500000 OR load_mwh <= 0;
""")

print("base_combine table created.")
