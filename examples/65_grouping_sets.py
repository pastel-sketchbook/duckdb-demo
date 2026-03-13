"""GROUPING SETS, ROLLUP & CUBE — subtotals and grand totals in a single query.

Ever built a sales report and needed subtotals per product, per customer,
AND a grand total?  The naive approach uses UNION ALL to glue separate
queries together.  GROUPING SETS replaces that with a single, declarative
query -- and ROLLUP / CUBE are shortcuts for the most common patterns.

Demonstrates how to:
  1. Solve the subtotals problem (naive UNION ALL vs GROUPING SETS)
  2. Use GROUPING SETS for explicit grouping combinations
  3. Use ROLLUP for hierarchical subtotals
  4. Use CUBE for every possible grouping combination
  5. Use GROUPING() to distinguish subtotal NULLs from real NULLs
  6. Build a polished sales report with labeled subtotal/total rows

Run with:
    uv run python examples/65_grouping_sets.py
"""

from __future__ import annotations

from pathlib import Path

import duckdb

# -- Setup --------------------------------------------------------------------
DATA_DIR = Path(__file__).resolve().parent.parent / "src" / "duckdb_demo" / "data"
CSV_FILE = DATA_DIR / "sales.csv"

conn = duckdb.connect()
conn.execute(
    f"""
    CREATE TABLE sales AS
    SELECT * FROM read_csv('{CSV_FILE}', auto_detect = true)
    """
)

# Quick peek at the data we're working with.
print("=== Source data (sales.csv) ===")
conn.sql("SELECT * FROM sales ORDER BY date").show()


# =============================================================================
# 1. The Problem — you want subtotals AND a grand total
# =============================================================================
# The naive way: run three separate aggregations and UNION ALL them together.
# It works, but it's verbose, repeats the SUM expression, and scans the table
# three times.

print("=== 1. Naive UNION ALL approach (verbose, 3 table scans) ===")
conn.sql(
    """
    -- Detail: revenue per product
    SELECT product, SUM(quantity * price) AS revenue
    FROM sales
    GROUP BY product

    UNION ALL

    -- Grand total: NULL product means "all products"
    SELECT NULL AS product, SUM(quantity * price) AS revenue
    FROM sales

    ORDER BY product NULLS LAST
    """
).show()
# Problem: imagine adding subtotals by customer, by month, etc. — the query
# explodes in size.  GROUPING SETS solves this cleanly.


# =============================================================================
# 2. GROUPING SETS — explicit control over grouping combinations
# =============================================================================
# GROUPING SETS lets you list exactly which GROUP BY combinations you want
# computed, all in a single pass over the data.
#
# Syntax:
#   GROUP BY GROUPING SETS ( (col_a, col_b), (col_a), () )
#
# Each tuple is one grouping level.  () means "no grouping" = grand total.

print("=== 2. GROUPING SETS — same result, one query, one scan ===")
conn.sql(
    """
    SELECT
        product,
        SUM(quantity * price) AS revenue
    FROM sales
    GROUP BY GROUPING SETS (
        (product),   -- one row per product  (subtotals)
        ()           -- one row with no grouping (grand total)
    )
    ORDER BY product NULLS LAST
    """
).show()

# You can list arbitrary combinations.  Here we get:
#   - revenue by (customer, product) — the detail
#   - revenue by (product) alone     — product subtotals
#   - revenue by (customer) alone    — customer subtotals
#   - grand total                    — ()
print("=== GROUPING SETS with multiple combinations ===")
conn.sql(
    """
    SELECT
        customer,
        product,
        SUM(quantity * price) AS revenue
    FROM sales
    GROUP BY GROUPING SETS (
        (customer, product),  -- full detail
        (product),            -- product subtotals
        (customer),           -- customer subtotals
        ()                    -- grand total
    )
    ORDER BY customer NULLS LAST, product NULLS LAST
    """
).show()


# =============================================================================
# 3. ROLLUP — hierarchical subtotals (most common in reports)
# =============================================================================
# ROLLUP(a, b) is shorthand for GROUPING SETS ((a, b), (a), ()).
# It peels off columns from right to left, giving you a hierarchy:
#   month → product → grand total
#
# This matches how business reports typically nest subtotals.

print("=== 3. ROLLUP — hierarchical subtotals ===")
conn.sql(
    """
    SELECT
        DATE_TRUNC('month', date) AS month,
        product,
        SUM(quantity * price)     AS revenue,
        SUM(quantity)             AS units
    FROM sales
    GROUP BY ROLLUP (month, product)
    ORDER BY month NULLS LAST, product NULLS LAST
    """
).show()
# Reading the output:
#   - Rows with both month + product filled → detail rows
#   - Rows with month filled, product NULL  → monthly subtotals
#   - Row with both NULL                    → grand total


# =============================================================================
# 4. CUBE — all possible combinations of grouping columns
# =============================================================================
# CUBE(a, b) is shorthand for GROUPING SETS ((a, b), (a), (b), ()).
# It gives you every possible subset of the grouping columns.
# With 2 columns that's 4 combinations; with 3 columns it's 8, etc.
# Use CUBE when you want a full cross-tab of subtotals.

print("=== 4. CUBE — every combination of customer × product ===")
conn.sql(
    """
    SELECT
        customer,
        product,
        SUM(quantity * price) AS revenue,
        COUNT(*)              AS order_count
    FROM sales
    GROUP BY CUBE (customer, product)
    ORDER BY customer NULLS LAST, product NULLS LAST
    """
).show()
# CUBE produces:
#   (customer, product) — detail rows
#   (customer, NULL)    — customer subtotals (all products)
#   (NULL, product)     — product subtotals  (all customers)
#   (NULL, NULL)        — grand total


# =============================================================================
# 5. GROUPING() function — distinguish subtotal NULLs from real NULLs
# =============================================================================
# When a column is NULL in the output, is it because the source data had a NULL
# or because this is a subtotal row?  The GROUPING() function answers that:
#   GROUPING(col) = 1  → this column was aggregated away (subtotal NULL)
#   GROUPING(col) = 0  → this column is part of the grouping (real value)
#
# This is critical when your data actually contains NULLs.

print("=== 5. GROUPING() function — subtotal detection ===")
conn.sql(
    """
    SELECT
        customer,
        product,
        SUM(quantity * price)  AS revenue,
        GROUPING(customer)     AS is_customer_subtotal,
        GROUPING(product)      AS is_product_subtotal
    FROM sales
    GROUP BY CUBE (customer, product)
    ORDER BY
        GROUPING(customer),    -- real groups first, then subtotals
        GROUPING(product),
        customer, product
    """
).show()
# When both GROUPING columns return 1, it's the grand total.
# When only is_product_subtotal = 1, it's a per-customer subtotal.
# When only is_customer_subtotal = 1, it's a per-product subtotal.


# =============================================================================
# 6. Practical: sales report with product + customer subtotals + grand total
# =============================================================================
# A real sales report: monthly breakdown with product and customer subtotals.
# We use GROUPING SETS to produce exactly the rows a manager would expect.

print("=== 6. Practical sales report — subtotals by product & customer ===")
conn.sql(
    """
    SELECT
        product,
        customer,
        SUM(quantity)         AS total_units,
        SUM(quantity * price) AS total_revenue,
        COUNT(*)              AS num_orders
    FROM sales
    GROUP BY GROUPING SETS (
        (product, customer),  -- detail: each product × customer pair
        (product),            -- subtotal per product
        (customer),           -- subtotal per customer
        ()                    -- grand total
    )
    ORDER BY
        GROUPING(product),
        GROUPING(customer),
        product NULLS LAST,
        customer NULLS LAST
    """
).show()


# =============================================================================
# 7. Labeling subtotal rows — CASE + GROUPING() for clean output
# =============================================================================
# The NULLs in subtotal rows are confusing in a report.  Best practice:
# use CASE WHEN GROUPING(col) = 1 THEN 'ALL' ELSE col END to label them.
# This turns raw NULL subtotals into human-friendly labels.

print("=== 7. Polished report: NULLs replaced with labels ===")
conn.sql(
    """
    SELECT
        -- Label subtotal rows clearly
        CASE WHEN GROUPING(product)  = 1 THEN '-- ALL PRODUCTS --'
             ELSE product
        END AS product,
        CASE WHEN GROUPING(customer) = 1 THEN '-- ALL CUSTOMERS --'
             ELSE customer
        END AS customer,
        SUM(quantity)                    AS total_units,
        ROUND(SUM(quantity * price), 2)  AS total_revenue,
        COUNT(*)                         AS num_orders,
        -- Add a row-type indicator for downstream processing
        CASE
            WHEN GROUPING(product) = 1 AND GROUPING(customer) = 1
                THEN 'GRAND TOTAL'
            WHEN GROUPING(customer) = 1
                THEN 'PRODUCT SUBTOTAL'
            WHEN GROUPING(product) = 1
                THEN 'CUSTOMER SUBTOTAL'
            ELSE 'DETAIL'
        END AS row_type
    FROM sales
    GROUP BY ROLLUP (product, customer)
    ORDER BY
        GROUPING(product),
        product,
        GROUPING(customer),
        customer
    """
).show()
# Now the report reads cleanly:
#   - DETAIL rows show each product × customer combination
#   - PRODUCT SUBTOTAL rows summarise each product across all customers
#   - GRAND TOTAL is the bottom line

# -- Bonus: same technique with CUBE for the full cross-tab ----
print("=== Bonus: full CUBE report with labels ===")
conn.sql(
    """
    SELECT
        CASE WHEN GROUPING(product)  = 1 THEN '** TOTAL **'
             ELSE product
        END AS product,
        CASE WHEN GROUPING(customer) = 1 THEN '** TOTAL **'
             ELSE customer
        END AS customer,
        SUM(quantity)                    AS units,
        ROUND(SUM(quantity * price), 2)  AS revenue,
        CASE
            WHEN GROUPING(product) = 1 AND GROUPING(customer) = 1
                THEN 'GRAND TOTAL'
            WHEN GROUPING(product) = 1  THEN 'CUSTOMER TOTAL'
            WHEN GROUPING(customer) = 1 THEN 'PRODUCT TOTAL'
            ELSE ''
        END AS row_type
    FROM sales
    GROUP BY CUBE (product, customer)
    ORDER BY
        GROUPING(product),
        product,
        GROUPING(customer),
        customer
    """
).show()

conn.close()
