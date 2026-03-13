"""Window functions — aggregates: running totals, moving averages, LAG, LEAD.

Shows how window aggregate functions compute values *across* rows without
collapsing the result set. Covers frame clauses (ROWS BETWEEN), LAG/LEAD
for row-to-row comparisons, and FIRST_VALUE/LAST_VALUE.

Run with:
    uv run python examples/32_window_aggregates.py
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

# Build a monthly summary to make window frames easy to visualise.
conn.execute(
    """
    CREATE TABLE monthly_revenue AS
    SELECT
        DATE_TRUNC('month', date) AS month,
        SUM(quantity * price)     AS revenue,
        COUNT(*)                  AS orders
    FROM sales
    GROUP BY month
    ORDER BY month
    """
)

print("=== Monthly revenue (base data for all examples) ===")
conn.sql("SELECT * FROM monthly_revenue ORDER BY month").show()

# -- 1. Running total (cumulative sum) ----------------------------------------
# SUM() OVER (ORDER BY ...) without a frame clause defaults to
# RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW — i.e. a running total.
print("=== Running total (cumulative revenue) ===")
conn.sql(
    """
    SELECT
        month,
        revenue,
        SUM(revenue) OVER (ORDER BY month) AS cumulative_revenue
    FROM monthly_revenue
    ORDER BY month
    """
).show()

# -- 2. Moving average (sliding window) ---------------------------------------
# ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING creates a 3-month centered window.
# The frame clause is the key concept: it controls *which rows* participate.
print("=== 3-month centered moving average ===")
conn.sql(
    """
    SELECT
        month,
        revenue,
        ROUND(
            AVG(revenue) OVER (
                ORDER BY month
                ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
            ), 2
        ) AS moving_avg_3m
    FROM monthly_revenue
    ORDER BY month
    """
).show()

# -- 3. LAG — peek at the previous row ----------------------------------------
# LAG(col, n, default) returns the value from n rows before the current row.
# Perfect for period-over-period comparisons (month-over-month growth, etc.).
print("=== LAG: month-over-month revenue change ===")
conn.sql(
    """
    SELECT
        month,
        revenue,
        LAG(revenue, 1) OVER (ORDER BY month)  AS prev_month,
        ROUND(
            revenue - LAG(revenue, 1) OVER (ORDER BY month), 2
        ) AS mom_change,
        CASE
            WHEN LAG(revenue, 1) OVER (ORDER BY month) IS NULL THEN NULL
            WHEN revenue > LAG(revenue, 1) OVER (ORDER BY month) THEN 'up'
            WHEN revenue < LAG(revenue, 1) OVER (ORDER BY month) THEN 'down'
            ELSE 'flat'
        END AS trend
    FROM monthly_revenue
    ORDER BY month
    """
).show()

# -- 4. LEAD — peek at the next row -------------------------------------------
# LEAD is the mirror of LAG — it looks forward instead of backward.
print("=== LEAD: next month's revenue (forward look) ===")
conn.sql(
    """
    SELECT
        month,
        revenue,
        LEAD(revenue, 1) OVER (ORDER BY month)  AS next_month,
        LEAD(revenue, 1, 0) OVER (ORDER BY month) AS next_or_zero
    FROM monthly_revenue
    ORDER BY month
    """
).show()

# -- 5. FIRST_VALUE / LAST_VALUE — anchoring to window boundaries -------------
# Show each month's revenue alongside the first and last month's values.
print("=== FIRST_VALUE / LAST_VALUE over the entire dataset ===")
conn.sql(
    """
    SELECT
        month,
        revenue,
        FIRST_VALUE(revenue) OVER (ORDER BY month) AS first_month_rev,
        LAST_VALUE(revenue) OVER (
            ORDER BY month
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS last_month_rev
    FROM monthly_revenue
    ORDER BY month
    """
).show()

# -- 6. Partitioned window aggregates -----------------------------------------
# PARTITION BY splits the window into independent groups — like GROUP BY but
# without collapsing rows.
print("=== Running total per product (PARTITION BY product) ===")
conn.sql(
    """
    SELECT
        date,
        product,
        quantity * price AS revenue,
        SUM(quantity * price) OVER (
            PARTITION BY product
            ORDER BY date
        ) AS product_running_total
    FROM sales
    ORDER BY product, date
    """
).show()

# -- 7. Percentage of partition total ------------------------------------------
# A common pattern: show each row as a percentage of its group's total.
print("=== Each order as % of its product's total revenue ===")
conn.sql(
    """
    SELECT
        date,
        customer,
        product,
        quantity * price AS revenue,
        ROUND(
            100.0 * (quantity * price) / SUM(quantity * price) OVER (PARTITION BY product),
            1
        ) AS pct_of_product
    FROM sales
    ORDER BY product, pct_of_product DESC
    """
).show()

# -- 8. Frame clause comparison -----------------------------------------------
# Show how different frame clauses change the result for the same SUM() window.
print("=== Frame clause comparison (same data, different windows) ===")
conn.sql(
    """
    SELECT
        month,
        revenue,
        -- Running total (default: UNBOUNDED PRECEDING to CURRENT ROW)
        SUM(revenue) OVER (ORDER BY month) AS running_total,
        -- Trailing 2-month sum
        SUM(revenue) OVER (
            ORDER BY month ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
        ) AS trailing_2m,
        -- Full partition sum (every row sees the grand total)
        SUM(revenue) OVER () AS grand_total
    FROM monthly_revenue
    ORDER BY month
    """
).show()

conn.close()
