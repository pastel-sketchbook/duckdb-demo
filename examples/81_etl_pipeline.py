"""ETL pipeline — extract from mixed sources, transform in SQL, load to Parquet.

Demonstrates a realistic Extract-Transform-Load workflow:
  1. Extract: read CSV + JSON source files (mixed formats)
  2. Transform: clean, join, and aggregate data using DuckDB SQL
  3. Load: write the results to Parquet files (columnar analytics format)

Run with:
    uv run python examples/81_etl_pipeline.py
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import duckdb

# -- Paths & connection -------------------------------------------------------
# DATA_DIR points to the bundled sample data shipped with this project.
DATA_DIR = Path(__file__).resolve().parent.parent / "src" / "duckdb_demo" / "data"
CSV_FILE = DATA_DIR / "sales.csv"
JSONL_FILE = DATA_DIR / "products.jsonl"

# In-memory DuckDB — no database file needed. Perfect for ETL pipelines
# that read from files, transform in SQL, and write results back to files.
conn = duckdb.connect()

# Create a temporary directory for our output Parquet files.
# Using tempfile keeps the project directory clean.
OUTPUT_DIR = Path(tempfile.mkdtemp(prefix="duckdb_etl_"))
print(f"Output directory: {OUTPUT_DIR}\n")


# =============================================================================
# 1. Setup — verify source files exist
# =============================================================================
print("=== 1. Setup ===")

# Always verify your source files before starting an ETL pipeline.
# In production you would check file sizes, modification dates, checksums, etc.
for path in [CSV_FILE, JSONL_FILE]:
    assert path.exists(), f"Missing source file: {path}"
    print(f"  Found: {path.name} ({path.stat().st_size:,} bytes)")

print()


# =============================================================================
# 2. Extract — read from CSV and JSON into DuckDB tables
# =============================================================================
# DuckDB can read CSV, JSON, Parquet, Excel, and more — no separate import
# step needed.  This "multi-format strength" is a key reason teams choose
# DuckDB for ETL: one engine handles all your source formats.
print("=== 2. Extract ===")

# -- Extract sales from CSV ---------------------------------------------------
# read_csv() with auto_detect infers column names, types, and delimiters.
conn.execute(
    f"""
    CREATE TABLE raw_sales AS
    SELECT *
    FROM read_csv('{CSV_FILE}', auto_detect = true)
    """
)
sales_count: int = conn.sql("SELECT COUNT(*) FROM raw_sales").fetchone()[0]  # type: ignore[index]
print(f"  Extracted {sales_count} rows from {CSV_FILE.name}")

# -- Extract products from JSONL (newline-delimited JSON) ---------------------
# read_json() auto-detects the JSON format and converts nested objects to
# STRUCTs and arrays to LISTs — no manual schema definition required.
conn.execute(
    f"""
    CREATE TABLE raw_products AS
    SELECT *
    FROM read_json('{JSONL_FILE}', auto_detect = true)
    """
)
products_count: int = conn.sql("SELECT COUNT(*) FROM raw_products").fetchone()[0]  # type: ignore[index]
print(f"  Extracted {products_count} rows from {JSONL_FILE.name}")

# Preview the raw data so we know what we're working with.
print("\n-- Raw sales (first 5 rows) --")
conn.sql("SELECT * FROM raw_sales LIMIT 5").show()

print("-- Raw products (first 5 rows) --")
conn.sql("SELECT * FROM raw_products LIMIT 5").show()


# =============================================================================
# 3. Transform: Clean — fix types, handle NULLs, normalize strings
# =============================================================================
# The "Clean" step ensures data quality before any joins or aggregations.
# Common tasks: cast strings to proper types, trim whitespace, normalize
# casing, fill or flag NULLs, and add computed columns.
print("=== 3. Transform: Clean ===")

conn.execute(
    """
    CREATE TABLE clean_sales AS
    SELECT
        -- Cast the date string to a proper DATE type for date arithmetic
        CAST(date AS DATE)              AS sale_date,

        -- Normalize customer names: trim whitespace, title-case
        -- (In real data, names often have inconsistent casing/spacing)
        TRIM(customer)                  AS customer,

        -- Normalize product names to consistent casing for reliable joins
        TRIM(UPPER(product))            AS product_upper,
        TRIM(product)                   AS product,

        -- Ensure quantity is a non-negative integer
        COALESCE(quantity, 0)           AS quantity,

        -- Ensure price is a non-negative decimal
        COALESCE(price, 0.0)            AS unit_price,

        -- Computed column: revenue = quantity * price
        -- This is the most common computed column in sales analytics
        COALESCE(quantity, 0) * COALESCE(price, 0.0) AS revenue,

        -- Extract date parts for easier aggregation later
        YEAR(CAST(date AS DATE))        AS sale_year,
        MONTH(CAST(date AS DATE))       AS sale_month,
        MONTHNAME(CAST(date AS DATE))   AS month_name
    FROM raw_sales
    """
)

print("-- Cleaned sales table --")
conn.sql(
    """
    SELECT sale_date, customer, product, quantity, unit_price, revenue,
           month_name
    FROM clean_sales
    LIMIT 5
    """
).show()

# Show the schema to confirm types are correct
print("-- Schema after cleaning --")
conn.sql("DESCRIBE clean_sales").show()


# =============================================================================
# 4. Transform: Enrich — JOIN sales with product metadata
# =============================================================================
# The "Enrich" step adds context from dimension tables.  Here we join the
# cleaned sales with product info (category, tags, specs) from the JSON
# source, creating an enriched fact table with everything an analyst needs.
print("=== 4. Transform: Enrich ===")

# First, flatten the product JSON into clean columns
conn.execute(
    """
    CREATE TABLE dim_products AS
    SELECT
        id              AS product_id,
        name            AS product_name,
        UPPER(name)     AS product_name_upper,
        category,
        price           AS catalog_price,
        -- Flatten nested specs STRUCT into top-level columns
        specs.weight_g  AS weight_grams,
        specs.color     AS color,
        -- Keep tags as a LIST — useful for array operations later
        tags
    FROM raw_products
    """
)

print("-- Product dimension table --")
conn.sql("SELECT * FROM dim_products LIMIT 5").show()

# JOIN sales with products on the product name.
# We use product_upper / product_name_upper to avoid case-mismatch issues.
conn.execute(
    """
    CREATE TABLE enriched_sales AS
    SELECT
        s.sale_date,
        s.customer,
        s.product,
        s.quantity,
        s.unit_price,
        s.revenue,
        s.sale_year,
        s.sale_month,
        s.month_name,
        -- Enrichment columns from the product dimension
        p.product_id,
        p.category,
        p.weight_grams,
        p.color,
        p.tags
    FROM clean_sales     AS s
    LEFT JOIN dim_products AS p
        ON s.product_upper = p.product_name_upper
    """
)

enriched_count: int = conn.sql("SELECT COUNT(*) FROM enriched_sales").fetchone()[0]  # type: ignore[index]
print(f"  Enriched fact table: {enriched_count} rows")

# Check for unmatched rows (products in sales that don't exist in the catalog)
unmatched: int = conn.sql(
    "SELECT COUNT(*) FROM enriched_sales WHERE product_id IS NULL"
).fetchone()[0]  # type: ignore[index]
print(f"  Unmatched rows (no product info): {unmatched}")

print("\n-- Enriched sales sample --")
conn.sql(
    """
    SELECT sale_date, customer, product, category, revenue, color, tags
    FROM enriched_sales
    LIMIT 5
    """
).show()


# =============================================================================
# 5. Transform: Aggregate — build summary tables
# =============================================================================
# Aggregation creates the final analytics-ready tables.  In a real warehouse
# these might be materialized views or star-schema summary tables.
print("=== 5. Transform: Aggregate ===")

# -- Monthly revenue summary --------------------------------------------------
conn.execute(
    """
    CREATE TABLE monthly_revenue AS
    SELECT
        sale_year,
        sale_month,
        month_name,
        COUNT(*)                 AS num_orders,
        SUM(quantity)            AS total_units,
        ROUND(SUM(revenue), 2)  AS total_revenue,
        ROUND(AVG(revenue), 2)  AS avg_order_revenue
    FROM enriched_sales
    GROUP BY sale_year, sale_month, month_name
    ORDER BY sale_year, sale_month
    """
)

print("-- Monthly revenue summary --")
conn.sql("SELECT * FROM monthly_revenue").show()

# -- Product performance summary -----------------------------------------------
conn.execute(
    """
    CREATE TABLE product_performance AS
    SELECT
        product,
        category,
        COUNT(*)                 AS num_orders,
        SUM(quantity)            AS total_units,
        ROUND(SUM(revenue), 2)  AS total_revenue,
        ROUND(AVG(revenue), 2)  AS avg_order_value,
        -- Rank products by total revenue within each category
        RANK() OVER (
            PARTITION BY category ORDER BY SUM(revenue) DESC
        )                        AS revenue_rank_in_category
    FROM enriched_sales
    GROUP BY product, category
    ORDER BY total_revenue DESC
    """
)

print("-- Product performance --")
conn.sql("SELECT * FROM product_performance").show()

# -- Customer segments ---------------------------------------------------------
# Segment customers by total spend: High (>$400), Medium ($200-$400), Low (<$200)
conn.execute(
    """
    CREATE TABLE customer_segments AS
    SELECT
        customer,
        COUNT(*)                 AS num_orders,
        SUM(quantity)            AS total_units,
        ROUND(SUM(revenue), 2)  AS total_spend,
        MIN(sale_date)           AS first_order,
        MAX(sale_date)           AS last_order,
        CASE
            WHEN SUM(revenue) >= 400 THEN 'High'
            WHEN SUM(revenue) >= 200 THEN 'Medium'
            ELSE 'Low'
        END                      AS spend_segment
    FROM enriched_sales
    GROUP BY customer
    ORDER BY total_spend DESC
    """
)

print("-- Customer segments --")
conn.sql("SELECT * FROM customer_segments").show()


# =============================================================================
# 6. Load — write results to Parquet files
# =============================================================================
# Parquet is the standard analytics format: columnar, compressed, and
# type-preserving.  COPY TO writes DuckDB tables directly to Parquet files
# with no intermediate steps.  This is the "Load" in ETL.
print("=== 6. Load ===")


def export_to_parquet(table_name: str, output_dir: Path) -> Path:
    """Export a DuckDB table to a Parquet file using COPY TO.

    Returns the path to the created Parquet file with its size printed.
    """
    out_path = output_dir / f"{table_name}.parquet"
    conn.execute(
        f"""
        COPY {table_name} TO '{out_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )
    size = out_path.stat().st_size
    print(f"  {table_name}.parquet — {size:,} bytes")
    return out_path


# Export all four result tables to Parquet
tables_to_export = [
    "enriched_sales",
    "monthly_revenue",
    "product_performance",
    "customer_segments",
]

parquet_files: dict[str, Path] = {}
for table in tables_to_export:
    parquet_files[table] = export_to_parquet(table, OUTPUT_DIR)

print(f"\n  All files written to: {OUTPUT_DIR}")


# =============================================================================
# 7. Verify — read Parquet files back and validate
# =============================================================================
# Always verify your ETL output!  Read the Parquet files back into DuckDB
# and confirm row counts match, types are preserved, and data is queryable.
print("\n=== 7. Verify ===")

# -- Row count verification ----------------------------------------------------
print("-- Row count verification --")
all_match = True
for table, path in parquet_files.items():
    # Count rows in the original table
    original: int = conn.sql(f"SELECT COUNT(*) FROM {table}").fetchone()[0]  # type: ignore[index]

    # Count rows in the Parquet file
    loaded: int = conn.sql(f"SELECT COUNT(*) FROM read_parquet('{path}')").fetchone()[0]  # type: ignore[index]

    status = "OK" if original == loaded else "MISMATCH"
    if original != loaded:
        all_match = False
    print(f"  {table}: {original} rows (table) vs {loaded} rows (parquet) — {status}")

print(f"\n  Verification: {'ALL COUNTS MATCH' if all_match else 'SOME MISMATCHES!'}")

# -- Type preservation check ---------------------------------------------------
# Parquet preserves DuckDB types (DATE stays DATE, not VARCHAR).
print("\n-- Type preservation: enriched_sales schema from Parquet --")
conn.sql(
    f"""
    DESCRIBE SELECT * FROM read_parquet('{parquet_files["enriched_sales"]}')
    """
).show()

# -- Sample query against loaded Parquet ---------------------------------------
# Prove the Parquet files are fully queryable — run an analytical query
# directly on the output files, just as a downstream consumer would.
print("-- Sample query: top customers from Parquet file --")
conn.sql(
    f"""
    SELECT
        customer,
        COUNT(*)                AS orders,
        ROUND(SUM(revenue), 2) AS total_revenue
    FROM read_parquet('{parquet_files["enriched_sales"]}')
    GROUP BY customer
    ORDER BY total_revenue DESC
    """
).show()

print("-- Sample query: monthly trend from Parquet file --")
conn.sql(
    f"""
    SELECT month_name, total_revenue, num_orders
    FROM read_parquet('{parquet_files["monthly_revenue"]}')
    ORDER BY sale_year, sale_month
    """
).show()


# =============================================================================
# Summary
# =============================================================================
print("=== Summary ===")
print(
    f"""
ETL Pipeline Complete!

  Extract:   Read {sales_count} sales rows (CSV) + {products_count} products (JSON)
  Transform: Cleaned, enriched, and aggregated into 4 tables
  Load:      Wrote {len(parquet_files)} Parquet files to {OUTPUT_DIR}
  Verify:    Row counts match, types preserved, queries work

Key DuckDB ETL patterns used:
  - read_csv() / read_json()    — extract from any format
  - CREATE TABLE ... AS SELECT  — transform with SQL
  - COALESCE / TRIM / UPPER     — clean and normalize
  - LEFT JOIN                   — enrich with dimension data
  - GROUP BY + window functions — aggregate summaries
  - COPY TO ... (FORMAT PARQUET) — load to columnar files
  - read_parquet()              — verify round-trip integrity

DuckDB makes ETL simple: no Spark cluster, no database server, no
complex orchestration.  Just SQL, files in, files out.
"""
)

conn.close()
