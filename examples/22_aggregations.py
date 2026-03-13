"""Aggregations — GROUP BY, HAVING, and the essential aggregate functions.

Covers SUM, COUNT, AVG, MIN, MAX, COUNT(DISTINCT), GROUP BY, HAVING,
and DuckDB's GROUP BY ALL shortcut.

Run with:
    uv run python examples/22_aggregations.py
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

# -- 1. Basic aggregates — the building blocks --------------------------------
# These functions collapse many rows into a single summary value.
print("=== Overall summary (COUNT, SUM, AVG, MIN, MAX) ===")
conn.sql(
    """
    SELECT
        COUNT(*)                  AS total_orders,
        SUM(quantity)             AS total_units,
        SUM(quantity * price)     AS total_revenue,
        AVG(quantity * price)     AS avg_order_revenue,
        MIN(date)                 AS first_order,
        MAX(date)                 AS last_order
    FROM sales
    """
).show()

# -- 2. GROUP BY — split the data into buckets, then aggregate each one --------
# Without GROUP BY, aggregates summarise the entire table.
# With GROUP BY, they summarise *each group* independently.
print("=== Revenue by product ===")
conn.sql(
    """
    SELECT
        product,
        COUNT(*)              AS order_count,
        SUM(quantity * price) AS revenue
    FROM sales
    GROUP BY product
    ORDER BY revenue DESC
    """
).show()

# -- 3. GROUP BY multiple columns ---------------------------------------------
# Group by more than one column to get finer-grained breakdowns.
print("=== Revenue by customer and product ===")
conn.sql(
    """
    SELECT
        customer,
        product,
        SUM(quantity * price) AS revenue
    FROM sales
    GROUP BY customer, product
    ORDER BY customer, revenue DESC
    """
).show()

# -- 4. HAVING — filter *after* aggregation -----------------------------------
# WHERE filters rows before aggregation; HAVING filters groups after.
# Common mistake: trying to use WHERE on an aggregate — that won't work.
print("=== Products with total revenue > $200 (HAVING) ===")
conn.sql(
    """
    SELECT
        product,
        SUM(quantity * price) AS revenue
    FROM sales
    GROUP BY product
    HAVING SUM(quantity * price) > 200
    ORDER BY revenue DESC
    """
).show()

# -- 5. COUNT(DISTINCT) — unique value counts ---------------------------------
# COUNT(*) counts all rows; COUNT(DISTINCT col) counts unique values.
print("=== Distinct customers and products per month ===")
conn.sql(
    """
    SELECT
        DATE_TRUNC('month', date)   AS month,
        COUNT(*)                    AS orders,
        COUNT(DISTINCT customer)    AS unique_customers,
        COUNT(DISTINCT product)     AS unique_products
    FROM sales
    GROUP BY month
    ORDER BY month
    """
).show()

# -- 6. GROUP BY ALL — DuckDB's time-saving shortcut --------------------------
# Instead of listing every non-aggregate column, DuckDB can infer them.
# This is equivalent to GROUP BY product, but less typing and less error-prone.
print("=== GROUP BY ALL — infer grouping columns automatically ===")
conn.sql(
    """
    SELECT
        product,
        ROUND(AVG(quantity), 1) AS avg_qty,
        ROUND(AVG(price), 2)    AS avg_price
    FROM sales
    GROUP BY ALL
    ORDER BY avg_qty DESC
    """
).show()

# -- 7. Conditional aggregation — COUNT/SUM with FILTER -----------------------
# DuckDB supports the FILTER clause on aggregates (SQL standard but rare).
# This is cleaner than CASE WHEN inside SUM/COUNT.
print("=== Conditional aggregation with FILTER clause ===")
conn.sql(
    """
    SELECT
        customer,
        COUNT(*) AS total_orders,
        COUNT(*) FILTER (WHERE product = 'Widget')  AS widget_orders,
        COUNT(*) FILTER (WHERE product = 'Gadget')  AS gadget_orders,
        COUNT(*) FILTER (WHERE product = 'Gizmo')   AS gizmo_orders,
        SUM(quantity * price)
            FILTER (WHERE quantity >= 10)            AS bulk_revenue
    FROM sales
    GROUP BY customer
    ORDER BY customer
    """
).show()

# -- 8. Common aggregation patterns -------------------------------------------
# Percentage of total: divide each group's sum by the overall sum.
print("=== Revenue share by product (% of total) ===")
conn.sql(
    """
    SELECT
        product,
        SUM(quantity * price)                                       AS revenue,
        ROUND(
            100.0 * SUM(quantity * price) / (SELECT SUM(quantity * price) FROM sales),
            1
        )                                                           AS pct_of_total
    FROM sales
    GROUP BY product
    ORDER BY revenue DESC
    """
).show()

conn.close()
