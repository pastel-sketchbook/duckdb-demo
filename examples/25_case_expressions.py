"""CASE expressions — conditional logic inside SQL queries.

Covers CASE WHEN, simple CASE, COALESCE, NULLIF, DuckDB's IF(),
and conditional aggregation patterns.

Run with:
    uv run python examples/25_case_expressions.py
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

# -- 1. Searched CASE WHEN — the most flexible form ---------------------------
# Evaluates each WHEN condition top-to-bottom; returns the first match.
# Here: classify orders into size buckets based on quantity.
print("=== CASE WHEN: order size classification ===")
conn.sql(
    """
    SELECT
        date,
        customer,
        product,
        quantity,
        CASE
            WHEN quantity >= 20 THEN 'large'
            WHEN quantity >= 10 THEN 'medium'
            WHEN quantity >= 5  THEN 'small'
            ELSE 'micro'
        END AS order_size
    FROM sales
    ORDER BY quantity DESC
    """
).show()

# -- 2. Simple CASE — shorthand when comparing one column to fixed values ------
# Equivalent to a searched CASE, but more concise for equality checks.
print("=== Simple CASE: product category mapping ===")
conn.sql(
    """
    SELECT
        product,
        CASE product
            WHEN 'Widget' THEN 'Hardware'
            WHEN 'Gadget' THEN 'Electronics'
            WHEN 'Gizmo'  THEN 'Electronics'
            ELSE 'Unknown'
        END AS category,
        SUM(quantity * price) AS revenue
    FROM sales
    GROUP BY product
    ORDER BY revenue DESC
    """
).show()

# -- 3. COALESCE — return the first non-NULL value ----------------------------
# COALESCE(a, b, c) returns the first argument that isn't NULL.
# Perfect for providing default values.
print("=== COALESCE: filling NULL values with defaults ===")

# Add a row with NULL product to demonstrate
conn.execute("INSERT INTO sales VALUES ('2024-06-01', 'Eve', NULL, 1, 0.00)")

conn.sql(
    """
    SELECT
        date,
        customer,
        COALESCE(product, '(no product)') AS product,
        quantity
    FROM sales
    WHERE customer = 'Eve' OR product IS NULL
    """
).show()

# -- 4. NULLIF — return NULL if two values are equal --------------------------
# NULLIF(a, b) returns NULL when a = b, otherwise returns a.
# Useful for avoiding division by zero or ignoring sentinel values.
print("=== NULLIF: treat zero-quantity as NULL (avoid divide-by-zero) ===")
conn.sql(
    """
    SELECT
        customer,
        product,
        quantity,
        -- Without NULLIF, this would error on quantity = 0
        ROUND(100.0 / NULLIF(quantity, 0), 2) AS pct_per_unit
    FROM sales
    WHERE product IS NOT NULL
    ORDER BY quantity ASC
    LIMIT 5
    """
).show()

# -- 5. DuckDB's IF() function — ternary shortcut -----------------------------
# IF(condition, true_value, false_value) is DuckDB's concise alternative to
# a two-branch CASE WHEN. Not standard SQL, but very convenient.
print("=== IF(): bulk order flag ===")
conn.sql(
    """
    SELECT
        date,
        customer,
        product,
        quantity,
        IF(quantity >= 10, 'bulk', 'regular') AS order_type
    FROM sales
    WHERE product IS NOT NULL
    ORDER BY date
    """
).show()

# -- 6. Conditional aggregation — CASE inside aggregate functions --------------
# Use CASE WHEN inside SUM/COUNT to aggregate only matching rows.
# This is the classic approach (FILTER clause in example 22 is cleaner).
print("=== Conditional aggregation: revenue by order size per customer ===")
conn.sql(
    """
    SELECT
        customer,
        SUM(CASE WHEN quantity >= 10 THEN quantity * price ELSE 0 END)
            AS bulk_revenue,
        SUM(CASE WHEN quantity < 10 THEN quantity * price ELSE 0 END)
            AS small_revenue,
        SUM(quantity * price) AS total_revenue
    FROM sales
    WHERE product IS NOT NULL
    GROUP BY customer
    ORDER BY total_revenue DESC
    """
).show()

# -- 7. CASE in ORDER BY — custom sort order ----------------------------------
# Sometimes you need a sort order that isn't alphabetical or numerical.
# CASE in ORDER BY lets you define a priority ranking inline.
print("=== CASE in ORDER BY: custom product priority ===")
conn.sql(
    """
    SELECT DISTINCT product
    FROM sales
    WHERE product IS NOT NULL
    ORDER BY
        CASE product
            WHEN 'Gizmo'  THEN 1   -- premium first
            WHEN 'Gadget' THEN 2
            WHEN 'Widget' THEN 3   -- basic last
            ELSE 4
        END
    """
).show()

# -- 8. Nested CASE — combining conditions for complex logic ------------------
# You can nest CASE expressions, though CTEs are often more readable.
print("=== Nested CASE: discount tier based on customer + quantity ===")
conn.sql(
    """
    SELECT
        customer,
        product,
        quantity,
        quantity * price AS subtotal,
        CASE
            WHEN quantity >= 20 THEN
                CASE
                    WHEN product = 'Gizmo' THEN '25% VIP discount'
                    ELSE '20% bulk discount'
                END
            WHEN quantity >= 10 THEN '10% volume discount'
            ELSE 'standard price'
        END AS discount_tier
    FROM sales
    WHERE product IS NOT NULL
    ORDER BY quantity DESC
    """
).show()

conn.close()
