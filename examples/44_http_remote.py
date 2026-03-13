"""HTTP & remote files — query data directly from URLs.

Covers the httpfs extension for reading Parquet/CSV from HTTPS URLs,
and demonstrates querying public datasets without downloading first.

Run with:
    uv run python examples/44_http_remote.py
"""

from __future__ import annotations

import duckdb

conn = duckdb.connect()

# -- 1. Install and load httpfs ------------------------------------------------
# The httpfs extension enables DuckDB to read files over HTTP/HTTPS.
# It's auto-installable — just INSTALL and LOAD.
conn.execute("INSTALL httpfs")
conn.execute("LOAD httpfs")
print("=== httpfs extension loaded ===\n")

# -- 2. Read a remote CSV directly ---------------------------------------------
# DuckDB can read CSV files from any HTTPS URL — no download step needed.
# Using a small public dataset from the DuckDB documentation samples.
print("=== Read a remote CSV from GitHub ===")
try:
    # Using a small CSV from the DuckDB samples repo
    url = "https://raw.githubusercontent.com/duckdb/duckdb-data/master/stations.csv"
    result = conn.sql(
        f"""
        SELECT station_id, name, lat, lon
        FROM read_csv('{url}', auto_detect = true)
        LIMIT 5
        """
    )
    result.show()
    print("(Showing first 5 rows from remote CSV)\n")
except Exception as e:
    print(f"Remote CSV read skipped (network may be unavailable): {e}\n")

# -- 3. Read a remote Parquet file ---------------------------------------------
# Parquet over HTTP benefits from column pruning and row-group filtering —
# DuckDB only downloads the bytes it needs, not the entire file.
print("=== Read remote Parquet (column pruning over HTTP) ===")
try:
    parquet_url = "https://github.com/duckdb/duckdb-data/raw/master/train_services.parquet"
    result = conn.sql(
        f"""
        SELECT COUNT(*) AS total_rows
        FROM read_parquet('{parquet_url}')
        """
    )
    result.show()

    # Column pruning: only download the columns we need
    conn.sql(
        f"""
        SELECT "Service:RDT station code", "Service:Type", COUNT(*) AS trips
        FROM read_parquet('{parquet_url}')
        GROUP BY ALL
        ORDER BY trips DESC
        LIMIT 5
        """
    ).show()
    print("(DuckDB only downloaded the columns it needed)\n")
except Exception as e:
    print(f"Remote Parquet read skipped (network may be unavailable): {e}\n")

# -- 4. Demonstrate with local fallback ----------------------------------------
# Since network access may not always be available, show the same patterns
# with inline data to ensure the example is always runnable.
print("=== Local fallback: same patterns without network ===")
conn.execute(
    """
    CREATE TABLE demo_remote AS
    SELECT * FROM (VALUES
        ('NYC', 'New York',      40.7128,  -74.0060, 8336817),
        ('LAX', 'Los Angeles',   34.0522, -118.2437, 3979576),
        ('CHI', 'Chicago',       41.8781,  -87.6298, 2693976),
        ('HOU', 'Houston',       29.7604,  -95.3698, 2320268),
        ('PHX', 'Phoenix',       33.4484, -112.0740, 1680992)
    ) AS t(code, city, lat, lon, population)
    """
)

# This works identically whether data came from HTTP or a local table
conn.sql(
    """
    SELECT code, city, population,
           RANK() OVER (ORDER BY population DESC) AS pop_rank
    FROM demo_remote
    ORDER BY pop_rank
    """
).show()

# -- 5. Key takeaways for remote data ------------------------------------------
print("=== Key takeaways ===")
print("  1. INSTALL httpfs; LOAD httpfs;  -- enables HTTP/HTTPS reads")
print("  2. read_csv('https://...')       -- works like a local file")
print("  3. read_parquet('https://...')   -- column pruning saves bandwidth")
print("  4. Wrap remote reads in try/except for offline resilience")
print("  5. For S3/GCS, use the aws or gcs extensions instead of httpfs")

conn.close()
