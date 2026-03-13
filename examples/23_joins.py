"""JOINs — combining data from multiple tables.

Covers INNER JOIN, LEFT JOIN, FULL OUTER JOIN, CROSS JOIN,
SEMI JOIN, and ANTI JOIN with practical examples.

Run with:
    uv run python examples/23_joins.py
"""

from __future__ import annotations

from pathlib import Path

import duckdb

# -- Setup: load sales + products into in-memory tables ------------------------
DATA_DIR = Path(__file__).resolve().parent.parent / "src" / "duckdb_demo" / "data"
CSV_FILE = DATA_DIR / "sales.csv"
JSONL_FILE = DATA_DIR / "products.jsonl"

conn = duckdb.connect()

# Sales table (15 rows of transactions)
conn.execute(
    f"""
    CREATE TABLE sales AS
    SELECT * FROM read_csv('{CSV_FILE}', auto_detect = true)
    """
)

# Products table (10 rows from JSONL — id, name, category, price, tags, specs)
conn.execute(
    f"""
    CREATE TABLE products AS
    SELECT id, name, category, price, tags
    FROM read_json('{JSONL_FILE}', auto_detect = true)
    """
)

# Quick peek at both tables
print("=== Sales (first 3 rows) ===")
conn.sql("SELECT * FROM sales LIMIT 3").show()
print("=== Products ===")
conn.sql("SELECT id, name, category, price FROM products").show()

# -- 1. INNER JOIN — only matching rows from both sides ----------------------
# The most common join: keep a row only when the join key exists in *both* tables.
# Here we join on product name so we can enrich sales with product category.
print("=== INNER JOIN: sales enriched with product category ===")
conn.sql(
    """
    SELECT
        s.date,
        s.customer,
        s.product,
        p.category,
        s.quantity,
        s.quantity * s.price AS revenue
    FROM sales s
    INNER JOIN products p
        ON s.product = p.name
    ORDER BY s.date
    LIMIT 8
    """
).show()

# -- 2. LEFT JOIN — all rows from the left, matches from the right -------------
# Use LEFT JOIN when you want to keep *every* row from the left table,
# even if there's no match on the right (unmatched right columns become NULL).
print("=== LEFT JOIN: all products, with sales totals (if any) ===")
conn.sql(
    """
    SELECT
        p.name                          AS product,
        p.category,
        COALESCE(SUM(s.quantity), 0)    AS units_sold,
        COALESCE(SUM(s.quantity * s.price), 0) AS revenue
    FROM products p
    LEFT JOIN sales s
        ON p.name = s.product
    GROUP BY p.name, p.category
    ORDER BY revenue DESC
    """
).show()

# -- 3. FULL OUTER JOIN — keep everything from both sides ---------------------
# Rows that don't match on either side will have NULLs for the other table's columns.
# Useful for reconciliation: "show me everything, matched or not."
print("=== FULL OUTER JOIN: products with/without sales, sales with/without products ===")
conn.sql(
    """
    SELECT
        COALESCE(p.name, s.product) AS product_name,
        p.category,
        s.customer,
        s.quantity
    FROM products p
    FULL OUTER JOIN sales s
        ON p.name = s.product
    ORDER BY product_name
    LIMIT 10
    """
).show()

# -- 4. CROSS JOIN — every combination of rows --------------------------------
# Cross join produces the Cartesian product. Usually you want a small table
# on at least one side. Useful for generating grids or lookup combinations.
print("=== CROSS JOIN: every customer × product combination (first 10) ===")
conn.sql(
    """
    SELECT
        c.customer,
        p.name AS product
    FROM (SELECT DISTINCT customer FROM sales) c
    CROSS JOIN (SELECT DISTINCT name FROM products WHERE category = 'Electronics') p
    ORDER BY c.customer, p.name
    LIMIT 10
    """
).show()

# -- 5. SEMI JOIN — filter left table by existence in right -------------------
# A semi join keeps rows from the left table *only if* a match exists on the right,
# but does NOT add columns from the right table. Think of it as an "EXISTS" filter.
print("=== SEMI JOIN: products that have at least one sale ===")
conn.sql(
    """
    SELECT p.name, p.category, p.price
    FROM products p
    SEMI JOIN sales s
        ON p.name = s.product
    ORDER BY p.name
    """
).show()

# -- 6. ANTI JOIN — the opposite of SEMI JOIN ---------------------------------
# Anti join keeps rows from the left table that have NO match on the right.
# Perfect for finding missing or orphaned records.
print("=== ANTI JOIN: products with ZERO sales ===")
conn.sql(
    """
    SELECT p.name, p.category, p.price
    FROM products p
    ANTI JOIN sales s
        ON p.name = s.product
    ORDER BY p.name
    """
).show()

# -- 7. Self JOIN — joining a table to itself ---------------------------------
# Useful for comparing rows within the same table (e.g., finding pairs).
print("=== Self JOIN: customer pairs who bought the same product ===")
conn.sql(
    """
    SELECT DISTINCT
        s1.customer AS customer_a,
        s2.customer AS customer_b,
        s1.product
    FROM sales s1
    INNER JOIN sales s2
        ON s1.product = s2.product
       AND s1.customer < s2.customer    -- avoid duplicates (A,B) and (B,A)
    ORDER BY s1.product, customer_a
    """
).show()

# -- 8. JOIN with aggregation — a common pattern ------------------------------
# Join + GROUP BY is the bread-and-butter of analytics queries.
print("=== Revenue by product category (JOIN + GROUP BY) ===")
conn.sql(
    """
    SELECT
        p.category,
        COUNT(*)                  AS order_count,
        SUM(s.quantity * s.price) AS revenue
    FROM sales s
    INNER JOIN products p
        ON s.product = p.name
    GROUP BY p.category
    ORDER BY revenue DESC
    """
).show()

conn.close()
