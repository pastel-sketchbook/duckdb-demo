"""Filtering & sorting — the WHERE clause toolkit every analyst needs.

Covers WHERE, ORDER BY, LIMIT, OFFSET, BETWEEN, IN, LIKE, and IS NULL.

Run with:
    uv run python examples/21_filter_and_sort.py
"""

from __future__ import annotations

from pathlib import Path

import duckdb

# -- Setup: load the bundled sales data into an in-memory table ----------------
DATA_DIR = Path(__file__).resolve().parent.parent / "src" / "duckdb_demo" / "data"
CSV_FILE = DATA_DIR / "sales.csv"

conn = duckdb.connect()
conn.execute(
    f"""
    CREATE TABLE sales AS
    SELECT * FROM read_csv('{CSV_FILE}', auto_detect = true)
    """
)

# -- 1. Basic WHERE — equality and comparison operators ------------------------
# The simplest filter: find rows that match an exact value.
print("=== Orders by Alice ===")
conn.sql("SELECT * FROM sales WHERE customer = 'Alice' ORDER BY date").show()

# -- 2. AND / OR — combining conditions ---------------------------------------
# Combine filters with AND (both must be true) and OR (either one).
print("=== Widget orders with quantity >= 10 ===")
conn.sql(
    """
    SELECT date, customer, product, quantity
    FROM sales
    WHERE product = 'Widget'
      AND quantity >= 10
    ORDER BY quantity DESC
    """
).show()

# -- 3. BETWEEN — inclusive range filter ---------------------------------------
# BETWEEN is shorthand for `col >= low AND col <= high`. Great for date ranges.
print("=== Sales in February 2024 (using BETWEEN) ===")
conn.sql(
    """
    SELECT date, customer, product, quantity * price AS revenue
    FROM sales
    WHERE date BETWEEN '2024-02-01' AND '2024-02-29'
    ORDER BY date
    """
).show()

# -- 4. IN — match against a list of values -----------------------------------
# IN replaces multiple OR conditions. Cleaner and easier to maintain.
print("=== Orders for Widget or Gizmo (using IN) ===")
conn.sql(
    """
    SELECT date, customer, product, quantity
    FROM sales
    WHERE product IN ('Widget', 'Gizmo')
    ORDER BY date
    """
).show()

# -- 5. LIKE / ILIKE — pattern matching ---------------------------------------
# % matches any sequence of characters, _ matches exactly one.
# ILIKE is the case-insensitive version (DuckDB extension).
print("=== Customers whose name starts with 'A' or 'B' (LIKE) ===")
conn.sql(
    """
    SELECT DISTINCT customer
    FROM sales
    WHERE customer LIKE 'A%'
       OR customer LIKE 'B%'
    ORDER BY customer
    """
).show()

# -- 6. IS NULL / IS NOT NULL --------------------------------------------------
# NULL comparisons require IS / IS NOT — regular = won't work.
# Let's add a row with a NULL to demonstrate.
conn.execute("INSERT INTO sales VALUES ('2024-06-01', 'Eve', NULL, 1, 0.00)")

print("=== Rows with NULL product (IS NULL) ===")
conn.sql("SELECT * FROM sales WHERE product IS NULL").show()

print("=== Rows with non-NULL product (IS NOT NULL) — just the count ===")
conn.sql("SELECT COUNT(*) AS non_null_count FROM sales WHERE product IS NOT NULL").show()

# -- 7. ORDER BY — controlling sort direction ----------------------------------
# ASC (default) = smallest first, DESC = largest first.
# You can sort by multiple columns — the second column breaks ties.
print("=== Top 5 orders by revenue (ORDER BY ... DESC, LIMIT) ===")
conn.sql(
    """
    SELECT
        date,
        customer,
        product,
        quantity * price AS revenue
    FROM sales
    WHERE product IS NOT NULL
    ORDER BY revenue DESC, date ASC
    LIMIT 5
    """
).show()

# -- 8. LIMIT + OFFSET — pagination -------------------------------------------
# OFFSET skips rows, LIMIT caps the result. Together they implement pagination.
print("=== Page 2 of sales (rows 6-10, i.e. OFFSET 5 LIMIT 5) ===")
conn.sql(
    """
    SELECT date, customer, product, quantity
    FROM sales
    WHERE product IS NOT NULL
    ORDER BY date
    LIMIT 5 OFFSET 5
    """
).show()

# -- 9. NOT — negating conditions ----------------------------------------------
# NOT flips any Boolean condition. Combine with IN, LIKE, BETWEEN, etc.
print("=== Orders for products NOT IN ('Widget', 'Gadget') ===")
conn.sql(
    """
    SELECT date, customer, product, quantity * price AS revenue
    FROM sales
    WHERE product NOT IN ('Widget', 'Gadget')
      AND product IS NOT NULL
    ORDER BY date
    """
).show()

conn.close()
