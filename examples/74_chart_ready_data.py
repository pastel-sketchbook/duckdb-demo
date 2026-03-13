"""Chart-ready data with PIVOT — preparing DuckDB results for BI tools.

Demonstrates how to reshape and export data so external tools (Excel, Tableau,
Power BI) can consume it directly.  No visualization libraries needed — just
DuckDB SQL and file export.

Key concepts:
  - PIVOT to create wide-format cross-tabs (products as columns)
  - UNPIVOT to convert wide tables back to long format (Tableau/Power BI style)
  - Summary statistics tables for dashboards
  - Time-series rollups at multiple granularities
  - Star-schema dimension + fact tables for BI modeling

Run with:
    uv run python examples/74_chart_ready_data.py
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import duckdb

# -- Setup --------------------------------------------------------------------
DATA_DIR = Path(__file__).resolve().parent.parent / "src" / "duckdb_demo" / "data"
CSV_FILE = DATA_DIR / "sales.csv"

conn = duckdb.connect()

# Load sales data into a table with a computed revenue column.
conn.execute(
    f"""
    CREATE TABLE sales AS
    SELECT
        date,
        customer,
        product,
        quantity,
        price,
        quantity * price AS revenue
    FROM read_csv('{CSV_FILE}', auto_detect = true)
    """
)

# Create a temp directory for all exported files.
tmp = Path(tempfile.mkdtemp(prefix="duckdb_chart_ready_"))
print(f"Export directory: {tmp}\n")


# -- Helper: export a query to CSV and report file size -----------------------
def export_csv(query: str, filename: str, description: str) -> Path:
    """Run a COPY TO CSV and print the file path + size."""
    out_path = tmp / filename
    conn.execute(
        f"""
        COPY (
            {query}
        ) TO '{out_path}' (FORMAT CSV, HEADER TRUE)
        """
    )
    size = out_path.stat().st_size
    print(f"  -> Saved {description}: {out_path.name} ({size:,} bytes)")
    return out_path


# =============================================================================
# 1. Setup — verify the source data
# =============================================================================
print("=== 1. Source Data (sales.csv) ===")
conn.sql("SELECT * FROM sales ORDER BY date").show()
print(f"Total rows: {conn.sql('SELECT COUNT(*) FROM sales').fetchone()[0]}\n")  # type: ignore[index]


# =============================================================================
# 2. Wide Format with PIVOT — monthly revenue cross-tab
# =============================================================================
# BI tools like Excel love wide-format data: one column per product, one row
# per month.  PIVOT turns distinct values of a column into separate columns.
print("=== 2. Wide Format with PIVOT (monthly revenue cross-tab) ===")

# First, build a monthly summary in long format.
conn.execute(
    """
    CREATE TABLE monthly_product AS
    SELECT
        STRFTIME(DATE_TRUNC('month', date), '%Y-%m') AS month,
        product,
        SUM(revenue) AS revenue
    FROM sales
    GROUP BY month, product
    ORDER BY month, product
    """
)
print("Long format (before PIVOT):")
conn.sql("SELECT * FROM monthly_product ORDER BY month, product").show()

# PIVOT: products become columns, months become rows.
print("Wide format (after PIVOT):")
wide_query = """
    SELECT * FROM (
        PIVOT monthly_product
        ON product
        USING SUM(revenue)
        GROUP BY month
        ORDER BY month
    )
"""
conn.sql(wide_query).show()

# Export the wide-format cross-tab to CSV — ready for Excel import.
export_csv(wide_query, "wide_monthly_revenue.csv", "wide-format cross-tab")
print()


# =============================================================================
# 3. Long Format with UNPIVOT — Tableau / Power BI style
# =============================================================================
# Tableau and Power BI prefer long (tidy) format: one row per observation.
# UNPIVOT melts the wide columns back into rows.
print("=== 3. Long Format with UNPIVOT ===")

# Create the wide table so we can unpivot it.
conn.execute(
    """
    CREATE TABLE wide_revenue AS
    PIVOT monthly_product
    ON product
    USING SUM(revenue)
    GROUP BY month
    """
)

# UNPIVOT: turn product columns back into name/value rows.
# COLUMNS(* EXCLUDE month) automatically picks all product columns.
long_query = """
    SELECT month, product, revenue
    FROM (
        UNPIVOT wide_revenue
        ON COLUMNS(* EXCLUDE month)
        INTO
            NAME product
            VALUE revenue
    )
    ORDER BY month, product
"""
print("Long format (after UNPIVOT):")
conn.sql(long_query).show()

export_csv(long_query, "long_monthly_revenue.csv", "long-format for Tableau/Power BI")
print()


# =============================================================================
# 4. Summary Statistics Table — per-product metrics
# =============================================================================
# Dashboard KPI cards often need a single summary row per dimension.
print("=== 4. Summary Statistics (per-product metrics) ===")

summary_query = """
    SELECT
        product,
        SUM(revenue)                     AS total_revenue,
        ROUND(AVG(revenue), 2)           AS avg_order,
        MIN(revenue)                     AS min_order,
        MAX(revenue)                     AS max_order,
        COUNT(*)                         AS order_count
    FROM sales
    GROUP BY product
    ORDER BY total_revenue DESC
"""
conn.sql(summary_query).show()

export_csv(summary_query, "product_summary.csv", "per-product summary statistics")
print()


# =============================================================================
# 5. Time-Series Rollup — daily → weekly → monthly
# =============================================================================
# Different dashboards need different time grains.  Export each granularity
# as a separate CSV so the BI tool can pick the right one.
print("=== 5. Time-Series Rollups ===")

# Daily (raw data, grouped by date)
daily_query = """
    SELECT
        date                     AS period,
        'daily'                  AS grain,
        SUM(revenue)             AS total_revenue,
        SUM(quantity)            AS total_quantity,
        COUNT(*)                 AS order_count
    FROM sales
    GROUP BY date
    ORDER BY date
"""
print("Daily rollup:")
conn.sql(daily_query).show()
export_csv(daily_query, "rollup_daily.csv", "daily rollup")

# Weekly — DATE_TRUNC('week', date) snaps to Monday.
weekly_query = """
    SELECT
        DATE_TRUNC('week', date) AS period,
        'weekly'                 AS grain,
        SUM(revenue)             AS total_revenue,
        SUM(quantity)            AS total_quantity,
        COUNT(*)                 AS order_count
    FROM sales
    GROUP BY period
    ORDER BY period
"""
print("\nWeekly rollup:")
conn.sql(weekly_query).show()
export_csv(weekly_query, "rollup_weekly.csv", "weekly rollup")

# Monthly
monthly_query = """
    SELECT
        DATE_TRUNC('month', date) AS period,
        'monthly'                 AS grain,
        SUM(revenue)              AS total_revenue,
        SUM(quantity)             AS total_quantity,
        COUNT(*)                  AS order_count
    FROM sales
    GROUP BY period
    ORDER BY period
"""
print("\nMonthly rollup:")
conn.sql(monthly_query).show()
export_csv(monthly_query, "rollup_monthly.csv", "monthly rollup")
print()


# =============================================================================
# 6. BI-Ready Star Schema — dimension + fact tables
# =============================================================================
# A star schema separates descriptive attributes (dimensions) from measurable
# events (facts).  This is the standard modeling pattern for BI tools.
print("=== 6. Star Schema (dimension + fact tables) ===")

# -- dim_customer: one row per unique customer with a surrogate key -----------
conn.execute(
    """
    CREATE TABLE dim_customer AS
    SELECT
        ROW_NUMBER() OVER (ORDER BY customer) AS customer_id,
        customer                              AS customer_name
    FROM (SELECT DISTINCT customer FROM sales)
    """
)
print("dim_customer:")
conn.sql("SELECT * FROM dim_customer ORDER BY customer_id").show()

# -- dim_product: one row per unique product with a surrogate key -------------
conn.execute(
    """
    CREATE TABLE dim_product AS
    SELECT
        ROW_NUMBER() OVER (ORDER BY product) AS product_id,
        product                              AS product_name,
        -- Pull the unit price from the source data (constant per product here)
        ANY_VALUE(price)                     AS unit_price
    FROM sales
    GROUP BY product
    """
)
print("dim_product:")
conn.sql("SELECT * FROM dim_product ORDER BY product_id").show()

# -- fact_sales: transactional grain with foreign keys to dimensions ----------
conn.execute(
    """
    CREATE TABLE fact_sales AS
    SELECT
        ROW_NUMBER() OVER (ORDER BY s.date, s.customer, s.product)
            AS sale_id,
        s.date,
        dc.customer_id,
        dp.product_id,
        s.quantity,
        s.revenue
    FROM sales s
    JOIN dim_customer dc ON s.customer = dc.customer_name
    JOIN dim_product  dp ON s.product  = dp.product_name
    """
)
print("fact_sales:")
conn.sql("SELECT * FROM fact_sales ORDER BY sale_id").show()

# Verify the star schema with a join query — just like a BI tool would.
print("Star-schema join (what a BI tool sees):")
conn.sql(
    """
    SELECT
        f.date,
        dc.customer_name,
        dp.product_name,
        dp.unit_price,
        f.quantity,
        f.revenue
    FROM fact_sales     f
    JOIN dim_customer   dc ON f.customer_id = dc.customer_id
    JOIN dim_product    dp ON f.product_id  = dp.product_id
    ORDER BY f.date, dc.customer_name
    """
).show()

# Export each table as a separate CSV — ready for BI tool import.
export_csv(
    "SELECT * FROM dim_customer ORDER BY customer_id",
    "dim_customer.csv",
    "dimension: customers",
)
export_csv(
    "SELECT * FROM dim_product ORDER BY product_id",
    "dim_product.csv",
    "dimension: products",
)
export_csv(
    "SELECT * FROM fact_sales ORDER BY sale_id",
    "fact_sales.csv",
    "fact: sales",
)
print()

# -- Final summary: list all exported files -----------------------------------
print("=== Exported Files Summary ===")
for f in sorted(tmp.iterdir()):
    print(f"  {f.name:.<40s} {f.stat().st_size:>6,} bytes")

print(f"\nAll files saved to: {tmp}")
print("(Temp directory — will be cleaned up by the OS)")

conn.close()
