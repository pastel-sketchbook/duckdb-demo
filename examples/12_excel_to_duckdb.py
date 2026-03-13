"""Convert an Excel file to a persistent DuckDB database.

Demonstrates how to:
  1. Install & load DuckDB's built-in ``excel`` extension
  2. Read an ``.xlsx`` file directly with ``read_xlsx()``
  3. Explore the data with SQL (filter, aggregate)
  4. Persist the result to a ``.duckdb`` file for later use

Run with:
    uv run python examples/12_excel_to_duckdb.py
"""

from __future__ import annotations

import os
from pathlib import Path

import duckdb

# -- Paths --------------------------------------------------------------------
DATA_DIR = Path(__file__).resolve().parent.parent / "src" / "duckdb_demo" / "data"
EXCEL_FILE = DATA_DIR / "inventory.xlsx"
DB_FILE = Path(__file__).resolve().parent / "inventory.duckdb"

# Clean up any leftover database from a previous run so the demo is repeatable.
if DB_FILE.exists():
    os.remove(DB_FILE)

# -- 1. Install & load the Excel extension ------------------------------------
# DuckDB ships an "excel" community extension.  Installing it once downloads
# the extension; loading it makes ``read_xlsx()`` available for the session.
conn = duckdb.connect(str(DB_FILE))
conn.execute("INSTALL excel;")
conn.execute("LOAD excel;")

# -- 2. Read the Excel file directly ------------------------------------------
# ``read_xlsx`` works just like ``read_csv`` -- no pandas or openpyxl needed at
# query time.  DuckDB handles the parsing internally.
print("=== Raw Excel data (first 5 rows) ===")
conn.sql(
    f"""
    SELECT *
    FROM read_xlsx('{EXCEL_FILE}', header = true)
    LIMIT 5
    """
).show()

# -- 3. Load into a permanent table -------------------------------------------
conn.execute(
    f"""
    CREATE TABLE inventory AS
    SELECT *
    FROM read_xlsx('{EXCEL_FILE}', header = true)
    """
)
row_count: int = conn.sql("SELECT COUNT(*) AS n FROM inventory").fetchone()[0]  # type: ignore[index]
print(f"\nLoaded {row_count} rows into table 'inventory'.")

# -- 4. Run a few queries on the new table ------------------------------------

# 4a. Low-stock items (quantity at or below reorder point)
print("\n=== Low-Stock Alert (quantity_on_hand <= reorder_point) ===")
conn.sql(
    """
    SELECT sku, product, warehouse, quantity_on_hand, reorder_point
    FROM inventory
    WHERE quantity_on_hand <= reorder_point
    ORDER BY quantity_on_hand
    """
).show()

# 4b. Total inventory value by category
print("=== Inventory Value by Category ===")
conn.sql(
    """
    SELECT
        category,
        SUM(quantity_on_hand)              AS total_units,
        ROUND(SUM(quantity_on_hand * unit_cost), 2) AS total_value
    FROM inventory
    GROUP BY category
    ORDER BY total_value DESC
    """
).show()

# 4c. Stock by warehouse
print("=== Stock Distribution by Warehouse ===")
conn.sql(
    """
    SELECT
        warehouse,
        COUNT(*)          AS product_count,
        SUM(quantity_on_hand) AS total_units
    FROM inventory
    GROUP BY warehouse
    ORDER BY total_units DESC
    """
).show()

# -- 5. Verify persistence ----------------------------------------------------
# Close and reopen the database to prove the data survived.
conn.close()

conn2 = duckdb.connect(str(DB_FILE), read_only=True)
print("=== Reopened database -- data persists ===")
conn2.sql("SELECT sku, product, category FROM inventory LIMIT 3").show()
conn2.close()

print(f"\nDone!  Database saved to: {DB_FILE}")
