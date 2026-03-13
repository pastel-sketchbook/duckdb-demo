"""Date & time functions — the temporal toolkit for analytics.

Covers date_trunc, date_part, date_diff, date_add, INTERVAL arithmetic,
strftime formatting, and DuckDB's friendly date literals.

Run with:
    uv run python examples/33_dates_and_times.py
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

# -- 1. DATE_TRUNC — snap dates to a boundary ---------------------------------
# date_trunc('month', date) returns the first day of that month.
# Essential for grouping events into time buckets.
print("=== DATE_TRUNC: monthly revenue ===")
conn.sql(
    """
    SELECT
        DATE_TRUNC('month', date)   AS month,
        COUNT(*)                    AS orders,
        SUM(quantity * price)       AS revenue
    FROM sales
    GROUP BY month
    ORDER BY month
    """
).show()

# Truncate to different granularities
print("=== DATE_TRUNC: quarter and year ===")
conn.sql(
    """
    SELECT DISTINCT
        date,
        DATE_TRUNC('quarter', date) AS quarter_start,
        DATE_TRUNC('year', date)    AS year_start
    FROM sales
    ORDER BY date
    LIMIT 6
    """
).show()

# -- 2. DATE_PART / EXTRACT — pull out individual components -------------------
# Returns an integer: year, month, day, hour, minute, dow (day of week), etc.
print("=== DATE_PART: extract components ===")
conn.sql(
    """
    SELECT
        date,
        DATE_PART('year', date)    AS year,
        DATE_PART('month', date)   AS month,
        DATE_PART('day', date)     AS day,
        DATE_PART('dow', date)     AS day_of_week,  -- 0=Sunday, 6=Saturday
        DATE_PART('week', date)    AS iso_week
    FROM sales
    ORDER BY date
    LIMIT 6
    """
).show()

# EXTRACT is the SQL-standard synonym — same result, different syntax.
print("=== EXTRACT (SQL-standard syntax) ===")
conn.sql(
    """
    SELECT
        date,
        EXTRACT(YEAR FROM date)  AS year,
        EXTRACT(MONTH FROM date) AS month
    FROM sales
    LIMIT 3
    """
).show()

# -- 3. DATE_DIFF — calculate the distance between two dates -------------------
# Returns the difference in the specified unit (days, months, years, etc.).
print("=== DATE_DIFF: days since first order ===")
conn.sql(
    """
    SELECT
        date,
        customer,
        DATE_DIFF('day',   (SELECT MIN(date) FROM sales), date) AS days_since_first,
        DATE_DIFF('month', (SELECT MIN(date) FROM sales), date) AS months_since_first
    FROM sales
    ORDER BY date
    """
).show()

# -- 4. DATE_ADD / INTERVAL — move dates forward or backward -------------------
# Add or subtract intervals from dates. DuckDB supports multiple syntaxes.
print("=== DATE_ADD and INTERVAL arithmetic ===")
conn.sql(
    """
    SELECT
        date                             AS original,
        date + INTERVAL 7 DAY            AS plus_7_days,
        date + INTERVAL 1 MONTH          AS plus_1_month,
        date - INTERVAL 1 YEAR           AS minus_1_year,
        DATE_ADD(date, INTERVAL 30 DAY)  AS date_add_30d
    FROM sales
    ORDER BY date
    LIMIT 5
    """
).show()

# -- 5. STRFTIME — format dates as strings ------------------------------------
# strftime uses C-style format codes: %Y=year, %m=month, %d=day, %B=month name.
print("=== STRFTIME: custom date formatting ===")
conn.sql(
    """
    SELECT
        date,
        STRFTIME(date, '%Y-%m-%d')   AS iso_format,
        STRFTIME(date, '%B %d, %Y')  AS long_format,
        STRFTIME(date, '%b %Y')      AS short_month_year,
        STRFTIME(date, '%A')         AS day_name
    FROM sales
    ORDER BY date
    LIMIT 5
    """
).show()

# -- 6. Date literals and CURRENT_DATE ----------------------------------------
# DuckDB understands date strings automatically. You can also use typed literals.
print("=== Date literals and CURRENT_DATE ===")
conn.sql(
    """
    SELECT
        CURRENT_DATE                        AS today,
        DATE '2024-01-01'                   AS new_year,
        TIMESTAMP '2024-06-15 14:30:00'     AS a_timestamp,
        DATE_DIFF('day', DATE '2024-01-01', CURRENT_DATE) AS days_since_ny
    """
).show()

# -- 7. Practical pattern: cohort analysis by first-purchase month -------------
# Find each customer's first purchase, then track subsequent orders as "months
# since first purchase." This is the foundation of cohort analysis.
print("=== Cohort analysis: months since first purchase ===")
conn.sql(
    """
    WITH first_purchase AS (
        SELECT customer, MIN(date) AS first_date
        FROM sales
        GROUP BY customer
    )
    SELECT
        s.customer,
        fp.first_date,
        s.date,
        DATE_DIFF('month', fp.first_date, s.date) AS months_since_first,
        s.quantity * s.price AS revenue
    FROM sales s
    JOIN first_purchase fp ON s.customer = fp.customer
    ORDER BY s.customer, s.date
    """
).show()

# -- 8. Generate a date series — fill gaps in sparse data ----------------------
# Real data often has missing dates. generate_series creates a continuous range.
print("=== Generate a date series to fill missing months ===")
conn.sql(
    """
    WITH all_months AS (
        SELECT UNNEST(
            generate_series(DATE '2024-01-01', DATE '2024-05-01', INTERVAL 1 MONTH)
        ) AS month
    ),
    actual AS (
        SELECT
            DATE_TRUNC('month', date) AS month,
            SUM(quantity * price)     AS revenue
        FROM sales
        GROUP BY month
    )
    SELECT
        am.month,
        COALESCE(a.revenue, 0) AS revenue,
        CASE WHEN a.revenue IS NULL THEN '(filled)' ELSE '' END AS note
    FROM all_months am
    LEFT JOIN actual a ON am.month = a.month
    ORDER BY am.month
    """
).show()

conn.close()
