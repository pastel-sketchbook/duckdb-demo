"""Friendly SQL extensions — DuckDB's quality-of-life syntax features.

Covers QUALIFY, SAMPLE, EXCLUDE, REPLACE, COLUMNS(), GROUP BY ALL,
ORDER BY ALL, and SELECT * REPLACE / EXCLUDE. These are DuckDB (or
modern SQL) features that most traditional databases lack.

Run with:
    uv run python examples/36_friendly_sql.py
"""

from __future__ import annotations

from pathlib import Path

import duckdb

# -- Setup ---------------------------------------------------------------------
DATA_DIR = Path(__file__).resolve().parent.parent / "src" / "duckdb_demo" / "data"
CSV_FILE = DATA_DIR / "sales.csv"

conn = duckdb.connect()
conn.execute(
    f"""
    CREATE TABLE sales AS
    SELECT * FROM read_csv('{CSV_FILE}', auto_detect = true)
    """
)

# -- 1. QUALIFY — filter on window function results directly -------------------
# Without QUALIFY you need a CTE or subquery to filter by ROW_NUMBER.
# QUALIFY lets you do it in one query — fewer lines, same result.

# Traditional approach (CTE):
print("=== Traditional: top-1 per product with CTE ===")
conn.sql(
    """
    WITH ranked AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product ORDER BY quantity DESC) AS rn
        FROM sales
    )
    SELECT date, customer, product, quantity FROM ranked WHERE rn = 1
    ORDER BY product
    """
).show()

# DuckDB's QUALIFY — same result, no CTE needed:
print("=== QUALIFY: top-1 per product (no CTE needed) ===")
conn.sql(
    """
    SELECT date, customer, product, quantity
    FROM sales
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product ORDER BY quantity DESC) = 1
    ORDER BY product
    """
).show()

# -- 2. SAMPLE — grab a random subset of rows ---------------------------------
# SAMPLE returns a random sample. You can specify a count or a percentage.
# Great for quick exploration of large datasets.
print("=== SAMPLE: random 5 rows ===")
conn.sql(
    """
    SELECT date, customer, product, quantity
    FROM sales
    USING SAMPLE 5
    """
).show()

print("=== SAMPLE: random 30% of rows ===")
conn.sql(
    """
    SELECT date, customer, product, quantity
    FROM sales
    USING SAMPLE 30 PERCENT
    """
).show()

# -- 3. EXCLUDE — select all columns except some ------------------------------
# SELECT * EXCLUDE (col1, col2) saves you from listing every column you *do* want.
print("=== EXCLUDE: all columns except price ===")
conn.sql(
    """
    SELECT * EXCLUDE (price)
    FROM sales
    LIMIT 5
    """
).show()

# -- 4. REPLACE — select all columns but transform some in place ---------------
# REPLACE modifies a column's expression while keeping its position in SELECT *.
print("=== REPLACE: round price to integer in place ===")
conn.sql(
    """
    SELECT * REPLACE (CAST(price AS INTEGER) AS price)
    FROM sales
    LIMIT 5
    """
).show()

# Combine EXCLUDE and REPLACE:
print("=== EXCLUDE + REPLACE: drop date, format quantity ===")
conn.sql(
    """
    SELECT * EXCLUDE (date) REPLACE (quantity || ' units' AS quantity)
    FROM sales
    LIMIT 5
    """
).show()

# -- 5. COLUMNS() — operate on multiple columns by pattern --------------------
# COLUMNS('regex') matches column names by a regular expression.
# Apply an expression or aggregate to all matching columns at once.
print("=== COLUMNS: MIN and MAX of all numeric columns ===")
conn.sql(
    """
    SELECT
        MIN(COLUMNS('quantity|price')),
        MAX(COLUMNS('quantity|price'))
    FROM sales
    """
).show()

# COLUMNS with a lambda — apply a transformation to each matched column:
print("=== COLUMNS with EXCLUDE: type of every column ===")
conn.sql(
    """
    SELECT TYPEOF(COLUMNS(*))
    FROM sales
    LIMIT 1
    """
).show()

# -- 6. GROUP BY ALL — auto-detect grouping columns ---------------------------
# GROUP BY ALL groups by every non-aggregate column in the SELECT list.
# Reduces boilerplate and prevents "column not in GROUP BY" errors.
print("=== GROUP BY ALL: no need to list grouping columns ===")
conn.sql(
    """
    SELECT
        customer,
        product,
        SUM(quantity * price) AS revenue
    FROM sales
    GROUP BY ALL
    ORDER BY revenue DESC
    """
).show()

# -- 7. ORDER BY ALL — sort by all columns in the SELECT list -----------------
# ORDER BY ALL orders by every selected column, left to right.
print("=== ORDER BY ALL: sort by all columns ===")
conn.sql(
    """
    SELECT DISTINCT customer, product
    FROM sales
    ORDER BY ALL
    """
).show()

# -- 8. String literal joins — join using column name strings ------------------
# DuckDB lets you join tables using a column name string with USING.
print("=== Simplified USING join ===")
conn.sql(
    """
    SELECT s.*, p.category
    FROM sales s
    INNER JOIN (
        SELECT DISTINCT name AS product, 'Electronics' AS category
        FROM (VALUES ('Gadget'), ('Gizmo')) t(name)
        UNION ALL
        SELECT 'Widget', 'Hardware'
    ) p
    USING (product)
    LIMIT 5
    """
).show()

# -- 9. FROM-first syntax — start with FROM, then SELECT ----------------------
# DuckDB allows writing FROM before SELECT. This can feel more natural when
# exploring data: "from this table, show me these columns."
print("=== FROM-first syntax ===")
conn.sql(
    """
    FROM sales
    SELECT customer, product, quantity * price AS revenue
    WHERE quantity > 5
    ORDER BY revenue DESC
    LIMIT 5
    """
).show()

conn.close()
