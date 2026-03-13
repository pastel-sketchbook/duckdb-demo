"""Subqueries & CTEs — structuring complex queries with reusable building blocks.

Covers scalar subqueries, correlated subqueries, EXISTS, IN (subquery),
and Common Table Expressions (WITH clauses) for readable, layered SQL.

Run with:
    uv run python examples/24_subqueries_ctes.py
"""

from __future__ import annotations

from pathlib import Path

import duckdb

# -- Setup: load the bundled sales data ---------------------------------------
DATA_DIR = Path(__file__).resolve().parent.parent / "src" / "duckdb_demo" / "data"
CSV_FILE = DATA_DIR / "sales.csv"

conn = duckdb.connect()
conn.execute(
    f"""
    CREATE TABLE sales AS
    SELECT * FROM read_csv('{CSV_FILE}', auto_detect = true)
    """
)

# -- 1. Scalar subquery — a subquery that returns a single value ---------------
# You can use a subquery anywhere a single value is expected — in SELECT, WHERE, etc.
# Here: compare each order's revenue against the overall average.
print("=== Scalar subquery: orders vs average revenue ===")
conn.sql(
    """
    SELECT
        date,
        customer,
        product,
        quantity * price AS revenue,
        (SELECT ROUND(AVG(quantity * price), 2) FROM sales) AS avg_revenue,
        CASE
            WHEN quantity * price > (SELECT AVG(quantity * price) FROM sales)
            THEN 'above avg'
            ELSE 'at/below avg'
        END AS vs_avg
    FROM sales
    ORDER BY revenue DESC
    """
).show()

# -- 2. Subquery in WHERE with IN — filter against a derived list -------------
# "Which customers bought the most expensive product?"
# Step 1: find the max-price product.  Step 2: filter sales to that product.
print("=== IN subquery: customers who bought the priciest product ===")
conn.sql(
    """
    SELECT DISTINCT customer
    FROM sales
    WHERE product IN (
        SELECT product
        FROM sales
        WHERE price = (SELECT MAX(price) FROM sales)
    )
    ORDER BY customer
    """
).show()

# -- 3. EXISTS — check whether *any* matching row exists ----------------------
# EXISTS returns TRUE as soon as the inner query returns at least one row.
# Useful for "has this customer ever bought product X?" style filters.
print("=== EXISTS: customers who have bought a Gizmo ===")
conn.sql(
    """
    SELECT DISTINCT s.customer
    FROM sales s
    WHERE EXISTS (
        SELECT 1
        FROM sales s2
        WHERE s2.customer = s.customer
          AND s2.product = 'Gizmo'
    )
    ORDER BY s.customer
    """
).show()

# -- 4. Correlated subquery — the inner query references the outer row --------
# For each sale, find the customer's *maximum* order revenue.
# The inner query runs once per outer row (conceptually — DuckDB optimizes this).
print("=== Correlated subquery: each order vs customer's best order ===")
conn.sql(
    """
    SELECT
        date,
        customer,
        product,
        quantity * price AS revenue,
        (
            SELECT MAX(s2.quantity * s2.price)
            FROM sales s2
            WHERE s2.customer = s.customer
        ) AS customer_best
    FROM sales s
    ORDER BY customer, date
    """
).show()

# -- 5. CTE (Common Table Expression) — the WITH clause ----------------------
# CTEs let you name intermediate result sets, making complex queries readable.
# Think of them as "inline temporary views" that exist only for one query.
print("=== CTE: monthly revenue summary ===")
conn.sql(
    """
    WITH monthly AS (
        SELECT
            DATE_TRUNC('month', date) AS month,
            SUM(quantity * price)     AS revenue,
            COUNT(*)                  AS orders
        FROM sales
        GROUP BY month
    )
    SELECT
        month,
        revenue,
        orders,
        ROUND(revenue / orders, 2) AS avg_order_value
    FROM monthly
    ORDER BY month
    """
).show()

# -- 6. Chained CTEs — building results layer by layer -----------------------
# You can define multiple CTEs separated by commas. Each can reference the ones
# defined before it. This is how you decompose a complex pipeline into steps.
print("=== Chained CTEs: customer lifetime value ranking ===")
conn.sql(
    """
    WITH
    -- Step 1: total revenue per customer
    customer_totals AS (
        SELECT
            customer,
            SUM(quantity * price) AS lifetime_value,
            COUNT(*)             AS order_count
        FROM sales
        GROUP BY customer
    ),
    -- Step 2: rank customers by lifetime value
    ranked AS (
        SELECT
            customer,
            lifetime_value,
            order_count,
            RANK() OVER (ORDER BY lifetime_value DESC) AS rank
        FROM customer_totals
    )
    SELECT * FROM ranked
    ORDER BY rank
    """
).show()

# -- 7. CTE vs subquery — same result, different readability ------------------
# This query answers "products whose revenue exceeds the average product revenue."
# Compare the CTE version (clear) with the nested subquery version (dense).

print("=== CTE version: above-average products ===")
conn.sql(
    """
    WITH product_rev AS (
        SELECT product, SUM(quantity * price) AS revenue
        FROM sales
        GROUP BY product
    )
    SELECT product, revenue
    FROM product_rev
    WHERE revenue > (SELECT AVG(revenue) FROM product_rev)
    ORDER BY revenue DESC
    """
).show()

# Same logic as a nested subquery (harder to follow):
print("=== Equivalent nested subquery (compare readability) ===")
conn.sql(
    """
    SELECT product, revenue
    FROM (
        SELECT product, SUM(quantity * price) AS revenue
        FROM sales
        GROUP BY product
    ) sub
    WHERE revenue > (
        SELECT AVG(revenue)
        FROM (
            SELECT SUM(quantity * price) AS revenue
            FROM sales
            GROUP BY product
        )
    )
    ORDER BY revenue DESC
    """
).show()

conn.close()
