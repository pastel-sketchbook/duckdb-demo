"""Synthetic data generation — create realistic test datasets with DuckDB.

Demonstrates DuckDB's data generation capabilities:
  1. generate_series() and range() for sequences
  2. random() and setseed() for reproducible randomness
  3. uuid() for unique identifiers
  4. List/array sampling for categorical data
  5. Date arithmetic for time-series data
  6. Building a complete synthetic dataset

Run with:
    uv run python examples/84_synthetic_data.py
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import duckdb

# =============================================================================
# 1. Setup — why synthetic data?
# =============================================================================
# Synthetic data is useful in many scenarios:
#   - Testing:  generate thousands of rows to stress-test queries and pipelines
#   - Demos:    create realistic-looking data without exposing real customer info
#   - Development: work offline with representative datasets
#   - Privacy:  share data patterns without revealing sensitive information
#
# DuckDB has built-in functions that make generating synthetic data easy —
# no external libraries needed.

print("=== 1. Setup ===")
print("Synthetic data lets you test, demo, and develop without real data.")
print("DuckDB can generate it entirely in SQL — no pandas or faker required.\n")

conn = duckdb.connect()

# =============================================================================
# 2. Sequences — generate_series() and range()
# =============================================================================
# generate_series() and range() produce rows of sequential values.
# They're the foundation for synthetic data — you start with N rows,
# then add random columns on top.
print("=== 2. Sequences — generate_series() and range() ===")

# generate_series(start, stop) — inclusive on both ends
print("--- generate_series(1, 10): inclusive range ---")
conn.sql(
    """
    SELECT generate_series AS id
    FROM generate_series(1, 10)
    """
).show()

# range(start, stop, step) — exclusive on the upper bound (like Python's range)
print("--- range(0, 50, 10): exclusive upper bound, custom step ---")
conn.sql(
    """
    SELECT range AS value
    FROM range(0, 50, 10)
    """
).show()

# Use generate_series as a row generator — the backbone of synthetic tables.
# Here we create 5 rows with a row number column.
print("--- Using generate_series as a row generator ---")
conn.sql(
    """
    SELECT generate_series AS row_num
    FROM generate_series(1, 5)
    """
).show()

# =============================================================================
# 3. Random values — random(), setseed(), random prices and integers
# =============================================================================
# random() returns a float in [0.0, 1.0).  Multiply and round to get
# values in any range.  Use setseed() before queries to make results
# reproducible — essential for tests and demos.
print("=== 3. Random Values — random() and setseed() ===")

# Without setseed(), random() gives different results each run.
# setseed(0.42) pins the PRNG so results are reproducible.
print("--- Reproducible random values with setseed(0.42) ---")
conn.sql(
    """
    SELECT setseed(0.42);
    """
)
conn.sql(
    """
    SELECT
        i AS row_num,
        -- Raw random float in [0, 1)
        ROUND(random(), 4)                    AS raw_random,
        -- Random price between 5.00 and 105.00
        ROUND(5.0 + random() * 100.0, 2)     AS random_price,
        -- Random integer between 1 and 10
        CAST(floor(1 + random() * 10) AS INTEGER) AS random_int,
        -- Random percentage 0–100
        ROUND(random() * 100, 1)              AS random_pct
    FROM generate_series(1, 8) AS t(i)
    """
).show()

# Demonstrate reproducibility: run the same seeded query twice.
# Call setseed() via conn.execute() before each query to ensure the PRNG
# is reset.  The random() calls then produce identical sequences.
print("--- Reproducibility check: same seed → same results ---")
conn.execute("SELECT setseed(0.42)")
result_a = conn.sql("SELECT ROUND(random(), 6) AS val FROM range(3)").fetchall()

conn.execute("SELECT setseed(0.42)")
result_b = conn.sql("SELECT ROUND(random(), 6) AS val FROM range(3)").fetchall()

print(f"  Run A: {[r[0] for r in result_a]}")
print(f"  Run B: {[r[0] for r in result_b]}")
print(f"  Match: {result_a == result_b}\n")

# =============================================================================
# 4. UUIDs and Categorical Sampling
# =============================================================================
# uuid() generates a unique identifier for each row — great for primary keys.
# To pick from a list of categorical values, index into an array literal
# using a random offset.
print("=== 4. UUIDs and Categorical Sampling ===")

# uuid() — each call produces a new unique identifier
print("--- uuid() for unique IDs ---")
conn.sql(
    """
    SELECT uuid() AS unique_id
    FROM generate_series(1, 5) AS t(i)
    """
).show()

# Categorical sampling: pick a random name from a fixed list.
# list_element(list, index) uses 1-based indexing.
# Formula: list_element(choices, CAST(floor(random() * len) AS INTEGER) + 1)
#   - random() returns [0.0, 1.0)
#   - floor(random() * 4) gives 0, 1, 2, or 3  (never 4)
#   - Add 1 for 1-based indexing → 1, 2, 3, or 4
#
# NOTE: Use floor() rather than a bare CAST, because CAST rounds and can
# produce an out-of-bounds index when random() is very close to 1.0.
print("--- Sampling from categorical lists ---")
conn.sql("SELECT setseed(0.42)")
conn.sql(
    """
    SELECT
        i AS row_num,
        -- Pick a random customer name (4 choices)
        list_element(
            ['Alice', 'Bob', 'Charlie', 'Diana'],
            CAST(floor(random() * 4) AS INTEGER) + 1
        ) AS customer,
        -- Pick a random product (5 choices)
        list_element(
            ['Widget', 'Gadget', 'Gizmo', 'Doohickey', 'Thingamajig'],
            CAST(floor(random() * 5) AS INTEGER) + 1
        ) AS product,
        -- Pick a random status (4 choices)
        list_element(
            ['pending', 'shipped', 'delivered', 'returned'],
            CAST(floor(random() * 4) AS INTEGER) + 1
        ) AS status
    FROM generate_series(1, 8) AS t(i)
    """
).show()

# =============================================================================
# 5. Date Generation — random dates and date spines
# =============================================================================
# DuckDB's date arithmetic makes it easy to generate random dates within a
# range or to create a "date spine" (one row per day/week/month).
print("=== 5. Date Generation ===")

# Random dates in 2024: add a random number of days to Jan 1
print("--- Random dates in a range ---")
conn.sql("SELECT setseed(0.42)")
conn.sql(
    """
    SELECT
        i AS row_num,
        -- Random date in 2024: base date + random offset of 0–364 days
        CAST(DATE '2024-01-01' + INTERVAL (CAST(floor(random() * 365) AS INTEGER)) DAY
             AS DATE) AS random_date
    FROM generate_series(1, 8) AS t(i)
    """
).show()

# Date spine: one row per day for January 2024
# generate_series works with dates and intervals too!
print("--- Date spine: every day in January 2024 ---")
conn.sql(
    """
    SELECT
        CAST(generate_series AS DATE) AS date,
        dayname(generate_series)      AS day_of_week
    FROM generate_series(
        DATE '2024-01-01',
        DATE '2024-01-31',
        INTERVAL 1 DAY
    )
    LIMIT 10
    """
).show()
print("  (showing first 10 of 31 days)\n")

# Monthly spine — useful for building time-series scaffolds
print("--- Monthly spine for 2024 ---")
conn.sql(
    """
    SELECT
        CAST(generate_series AS DATE)  AS month_start,
        monthname(generate_series)     AS month_name
    FROM generate_series(
        DATE '2024-01-01',
        DATE '2024-12-01',
        INTERVAL 1 MONTH
    )
    """
).show()

# =============================================================================
# 6. Complete Synthetic Dataset — a realistic "orders" table
# =============================================================================
# Now we combine every technique into a single query that generates
# 1000 realistic order rows.
print("=== 6. Complete Synthetic Dataset — 1000 Orders ===")

conn.sql("SELECT setseed(0.42)")
conn.execute(
    """
    CREATE TABLE synthetic_orders AS
    SELECT
        -- Unique order ID (UUID)
        uuid()  AS order_id,

        -- Random customer name sampled from a list of 6 names
        list_element(
            ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve', 'Frank'],
            CAST(floor(random() * 6) AS INTEGER) + 1
        ) AS customer_name,

        -- Random product sampled from a list of 5 products
        list_element(
            ['Widget', 'Gadget', 'Gizmo', 'Doohickey', 'Thingamajig'],
            CAST(floor(random() * 5) AS INTEGER) + 1
        ) AS product,

        -- Random order date in 2024
        CAST(
            DATE '2024-01-01'
            + INTERVAL (CAST(floor(random() * 365) AS INTEGER)) DAY
            AS DATE
        ) AS order_date,

        -- Random quantity between 1 and 20
        CAST(floor(1 + random() * 20) AS INTEGER) AS quantity,

        -- Random unit price between $5.00 and $200.00
        ROUND(5.0 + random() * 195.0, 2)  AS price

    FROM generate_series(1, 1000) AS t(i)
    """
)

# Preview the first few rows
print("--- Sample rows ---")
conn.sql("SELECT * FROM synthetic_orders LIMIT 10").show()

# SUMMARIZE gives a quick statistical profile of every column:
# count, min, max, mean, stddev, nulls, unique values, etc.
print("--- SUMMARIZE: statistical profile of the dataset ---")
conn.sql("SUMMARIZE synthetic_orders").show()

# Quick aggregations to sanity-check the data distribution
print("--- Distribution check: orders per customer ---")
conn.sql(
    """
    SELECT
        customer_name,
        COUNT(*)                            AS order_count,
        ROUND(AVG(quantity), 1)             AS avg_qty,
        ROUND(AVG(price), 2)               AS avg_price,
        ROUND(SUM(quantity * price), 2)    AS total_revenue
    FROM synthetic_orders
    GROUP BY customer_name
    ORDER BY total_revenue DESC
    """
).show()

print("--- Distribution check: orders per month ---")
conn.sql(
    """
    SELECT
        monthname(order_date)               AS month,
        MONTH(order_date)                   AS month_num,
        COUNT(*)                            AS order_count
    FROM synthetic_orders
    GROUP BY month, month_num
    ORDER BY month_num
    """
).show()

# =============================================================================
# 7. Export — write to Parquet, verify round-trip
# =============================================================================
# Write the synthetic dataset to a temporary Parquet file, check its size,
# then read it back to confirm the data survived the round-trip.
print("=== 7. Export to Parquet and Verify ===")

tmp = Path(tempfile.mkdtemp(prefix="duckdb_synthetic_"))
parquet_path = tmp / "synthetic_orders.parquet"

conn.execute(
    f"""
    COPY synthetic_orders
    TO '{parquet_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """
)

file_size = parquet_path.stat().st_size
print(f"Wrote {file_size:,} bytes to: {parquet_path}")
print(f"  ({file_size / 1024:.1f} KB for 1,000 rows)\n")

# Read the Parquet file back and verify row count + schema
print("--- Round-trip verification ---")
conn.sql(
    f"""
    SELECT COUNT(*) AS row_count
    FROM read_parquet('{parquet_path}')
    """
).show()

print("--- Schema of the Parquet file ---")
conn.sql(
    f"""
    DESCRIBE SELECT * FROM read_parquet('{parquet_path}')
    """
).show()

print(f"Temp files saved to: {tmp}")
print("(Will be cleaned up by the OS)\n")

conn.close()
print("Done! You now know how to generate synthetic data entirely in DuckDB.")
