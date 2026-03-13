"""Quick-start example: query a CSV file with DuckDB -- zero setup required.

Run with:
    uv run python examples/11_quick_start.py
"""

from __future__ import annotations

from pathlib import Path

import duckdb

# Point at the bundled sample data
DATA_DIR = Path(__file__).resolve().parent.parent / "src" / "duckdb_demo" / "data"
CSV_FILE = DATA_DIR / "sales.csv"

# -- 1. Query the CSV directly (no CREATE TABLE needed) -----------------------
print("=== Top 3 products by revenue (straight from CSV) ===")
duckdb.sql(
    f"""
    SELECT
        product,
        SUM(quantity * price) AS revenue
    FROM read_csv('{CSV_FILE}', auto_detect = true)
    GROUP BY product
    ORDER BY revenue DESC
    LIMIT 3
    """
).show()

# -- 2. Quick aggregation -----------------------------------------------------
print("=== Total rows and overall revenue ===")
duckdb.sql(
    f"""
    SELECT
        COUNT(*)              AS total_rows,
        SUM(quantity * price) AS total_revenue
    FROM read_csv('{CSV_FILE}', auto_detect = true)
    """
).show()
