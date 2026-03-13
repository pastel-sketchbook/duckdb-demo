"""Read and query Parquet files directly with DuckDB.

Demonstrates how to:
  1. Read a Parquet file with ``read_parquet()`` -- no loading step needed
  2. Inspect Parquet metadata (row groups, columns, compression)
  3. Use column pruning and row-group filtering for efficient reads
  4. Compare Parquet vs CSV for the same data

Run with:
    uv run python examples/13_read_parquet.py
"""

from __future__ import annotations

from pathlib import Path

import duckdb

# -- Paths --------------------------------------------------------------------
DATA_DIR = Path(__file__).resolve().parent.parent / "src" / "duckdb_demo" / "data"
PARQUET_FILE = DATA_DIR / "sales.parquet"
CSV_FILE = DATA_DIR / "sales.csv"

# -- 1. Query the Parquet file directly ---------------------------------------
# Just like read_csv(), DuckDB can query Parquet files without loading them
# into a table first.  Parquet is a columnar format -- DuckDB only reads the
# columns your query actually needs.
print("=== All sales from the Parquet file ===")
duckdb.sql(
    f"""
    SELECT *
    FROM read_parquet('{PARQUET_FILE}')
    LIMIT 5
    """
).show()

# -- 2. Inspect Parquet metadata ----------------------------------------------
# The parquet_metadata() function reveals internal details: row groups,
# compression codec, number of values, etc.
print("=== Parquet metadata (row groups & columns) ===")
duckdb.sql(
    f"""
    SELECT
        row_group_id,
        column_id,
        path_in_schema  AS column_name,
        type            AS physical_type,
        num_values,
        total_compressed_size   AS compressed_bytes,
        total_uncompressed_size AS raw_bytes
    FROM parquet_metadata('{PARQUET_FILE}')
    """
).show()

# -- 3. Column pruning -------------------------------------------------------
# When you SELECT only specific columns, DuckDB skips reading the rest from
# the Parquet file.  This is a big performance win on wide tables.
print("=== Column pruning: only product + revenue ===")
duckdb.sql(
    f"""
    SELECT
        product,
        SUM(quantity * price) AS revenue
    FROM read_parquet('{PARQUET_FILE}')
    GROUP BY product
    ORDER BY revenue DESC
    """
).show()

# -- 4. Row-group filtering (predicate pushdown) ------------------------------
# DuckDB pushes WHERE filters down into the Parquet reader so it can skip
# entire row groups whose min/max statistics prove no match.
print("=== Row-group filtering: only 2024-03 data ===")
duckdb.sql(
    f"""
    SELECT *
    FROM read_parquet('{PARQUET_FILE}')
    WHERE date >= '2024-03-01' AND date < '2024-04-01'
    ORDER BY date
    """
).show()

# -- 5. Parquet schema --------------------------------------------------------
print("=== Parquet schema (column names & types) ===")
duckdb.sql(
    f"""
    SELECT column_name, column_type
    FROM (DESCRIBE SELECT * FROM read_parquet('{PARQUET_FILE}'))
    """
).show()

# -- 6. Compare file sizes ----------------------------------------------------
csv_size = CSV_FILE.stat().st_size
parquet_size = PARQUET_FILE.stat().st_size
print(f"CSV file size:     {csv_size:>6,} bytes")
print(f"Parquet file size: {parquet_size:>6,} bytes")
print(f"Compression ratio: {csv_size / parquet_size:.1f}x smaller in Parquet")
