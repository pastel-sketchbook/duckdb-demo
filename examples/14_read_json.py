"""Read and query JSON files directly with DuckDB.

Demonstrates how to:
  1. Read newline-delimited JSON (JSONL) with ``read_json()``
  2. Auto-detect schema from JSON structure
  3. Access nested fields (objects and arrays) in SQL
  4. Filter and aggregate over JSON data

Run with:
    uv run python examples/14_read_json.py
"""

from __future__ import annotations

from pathlib import Path

import duckdb

# -- Paths --------------------------------------------------------------------
DATA_DIR = Path(__file__).resolve().parent.parent / "src" / "duckdb_demo" / "data"
JSONL_FILE = DATA_DIR / "products.jsonl"

# -- 1. Read the JSON file directly -------------------------------------------
# DuckDB auto-detects the JSON format (array-of-objects, newline-delimited,
# etc.) and infers the schema from the data.
print("=== Raw JSON data (first 5 rows) ===")
duckdb.sql(
    f"""
    SELECT *
    FROM read_json('{JSONL_FILE}', auto_detect = true)
    LIMIT 5
    """
).show()

# -- 2. Inspect the auto-detected schema -------------------------------------
# DuckDB converts nested JSON objects into STRUCTs and arrays into LISTs.
print("=== Auto-detected schema ===")
duckdb.sql(
    f"""
    SELECT column_name, column_type
    FROM (DESCRIBE SELECT * FROM read_json('{JSONL_FILE}', auto_detect = true))
    """
).show()

# -- 3. Access nested fields --------------------------------------------------
# The "specs" object becomes a STRUCT -- access fields with dot notation.
# The "tags" array becomes a LIST.
print("=== Nested field access: specs.weight_g and specs.color ===")
duckdb.sql(
    f"""
    SELECT
        name,
        category,
        price,
        specs.weight_g  AS weight_grams,
        specs.color      AS color
    FROM read_json('{JSONL_FILE}', auto_detect = true)
    ORDER BY weight_grams DESC
    """
).show()

# -- 4. Unnest arrays ---------------------------------------------------------
# Use unnest() to expand the tags LIST into individual rows.
print("=== Unnest tags: one row per product-tag pair ===")
duckdb.sql(
    f"""
    SELECT
        name,
        unnest(tags) AS tag
    FROM read_json('{JSONL_FILE}', auto_detect = true)
    ORDER BY name, tag
    """
).show()

# -- 5. Aggregate over JSON data ----------------------------------------------
print("=== Product count and avg price by category ===")
duckdb.sql(
    f"""
    SELECT
        category,
        COUNT(*)             AS num_products,
        ROUND(AVG(price), 2) AS avg_price,
        MIN(price)           AS min_price,
        MAX(price)           AS max_price
    FROM read_json('{JSONL_FILE}', auto_detect = true)
    GROUP BY category
    ORDER BY avg_price DESC
    """
).show()

# -- 6. Filter with nested predicates ----------------------------------------
print("=== Premium products (tag = 'premium') over $20 ===")
duckdb.sql(
    f"""
    SELECT name, price, tags
    FROM read_json('{JSONL_FILE}', auto_detect = true)
    WHERE list_contains(tags, 'premium')
      AND price > 20
    ORDER BY price DESC
    """
).show()
