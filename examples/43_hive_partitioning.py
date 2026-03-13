"""Hive-partitioned data — read and write directory-based partitions.

Covers hive_partitioning for reads, PARTITION_BY for writes, and how
DuckDB uses partition pruning to skip irrelevant files.

Run with:
    uv run python examples/43_hive_partitioning.py
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import duckdb

# -- Setup: load sales data and create a temp directory for partitioned output --
DATA_DIR = Path(__file__).resolve().parent.parent / "src" / "duckdb_demo" / "data"
CSV_FILE = DATA_DIR / "sales.csv"
tmp = Path(tempfile.mkdtemp(prefix="duckdb_hive_"))

conn = duckdb.connect()
conn.execute(
    f"""
    CREATE TABLE sales AS
    SELECT
        date,
        DATE_TRUNC('month', date) AS month,
        customer,
        product,
        quantity,
        price,
        quantity * price AS revenue
    FROM read_csv('{CSV_FILE}', auto_detect = true)
    """
)

# -- 1. Write Hive-partitioned Parquet with PARTITION_BY ----------------------
# COPY ... PARTITION_BY creates a directory tree like:
#   output/product=Widget/data_0.parquet
#   output/product=Gadget/data_0.parquet
#   output/product=Gizmo/data_0.parquet
part_dir = tmp / "by_product"
conn.execute(
    f"""
    COPY (
        SELECT date, customer, product, quantity, revenue
        FROM sales
    ) TO '{part_dir}' (
        FORMAT PARQUET,
        PARTITION_BY (product),
        OVERWRITE_OR_IGNORE
    )
    """
)

print("=== Hive-partitioned Parquet directory structure ===")
for p in sorted(part_dir.rglob("*.parquet")):
    rel = p.relative_to(part_dir)
    print(f"  {rel}  ({p.stat().st_size} bytes)")
print()

# -- 2. Read Hive-partitioned data back ----------------------------------------
# DuckDB auto-detects Hive partition columns from directory names.
print("=== Read partitioned data with hive_partitioning = true ===")
conn.sql(
    f"""
    SELECT *
    FROM read_parquet('{part_dir}/**/*.parquet', hive_partitioning = true)
    ORDER BY date
    """
).show()

# -- 3. Partition pruning — only read the partitions you need ------------------
# When you filter on the partition column, DuckDB skips reading irrelevant files.
print("=== Partition pruning: only read product = 'Widget' ===")
conn.sql(
    f"""
    SELECT date, customer, quantity, revenue
    FROM read_parquet('{part_dir}/**/*.parquet', hive_partitioning = true)
    WHERE product = 'Widget'
    ORDER BY date
    """
).show()

# -- 4. Multi-level partitioning — partition by product AND month ---------------
multi_dir = tmp / "by_product_month"
conn.execute(
    f"""
    COPY (
        SELECT date, customer, product,
               STRFTIME(month, '%Y-%m') AS month,
               quantity, revenue
        FROM sales
    ) TO '{multi_dir}' (
        FORMAT PARQUET,
        PARTITION_BY (product, month),
        OVERWRITE_OR_IGNORE
    )
    """
)

print("=== Multi-level partition directory structure ===")
for p in sorted(multi_dir.rglob("*.parquet")):
    rel = p.relative_to(multi_dir)
    print(f"  {rel}")
print()

# Read back with both partition columns
print("=== Read multi-level partitions ===")
conn.sql(
    f"""
    SELECT product, month, COUNT(*) AS orders, SUM(revenue) AS revenue
    FROM read_parquet('{multi_dir}/**/*.parquet', hive_partitioning = true)
    GROUP BY product, month
    ORDER BY product, month
    """
).show()

# -- 5. Write Hive-partitioned CSV (same syntax, different format) -------------
csv_part_dir = tmp / "csv_by_product"
conn.execute(
    f"""
    COPY (
        SELECT date, customer, product, quantity, revenue
        FROM sales
    ) TO '{csv_part_dir}' (
        FORMAT CSV,
        HEADER TRUE,
        PARTITION_BY (product),
        OVERWRITE_OR_IGNORE
    )
    """
)

print("=== CSV partitions ===")
for p in sorted(csv_part_dir.rglob("*.csv")):
    rel = p.relative_to(csv_part_dir)
    print(f"  {rel}")
print()

# -- 6. Practical pattern: append-only partition writes ------------------------
# In real ETL, you often write one partition at a time (e.g., daily ingestion).
# OVERWRITE_OR_IGNORE prevents errors if the partition already exists.
print("=== Append-only pattern: write a single partition ===")
append_dir = tmp / "incremental"
for product_filter in ["Widget", "Gadget", "Gizmo"]:
    conn.execute(
        f"""
        COPY (
            SELECT date, customer, product, quantity, revenue
            FROM sales
            WHERE product = '{product_filter}'
        ) TO '{append_dir}' (
            FORMAT PARQUET,
            PARTITION_BY (product),
            OVERWRITE_OR_IGNORE
        )
        """
    )
    print(f"  Wrote partition: product={product_filter}")

print("\nAll partitions combined:")
conn.sql(
    f"""
    SELECT product, COUNT(*) AS rows
    FROM read_parquet('{append_dir}/**/*.parquet', hive_partitioning = true)
    GROUP BY product
    ORDER BY product
    """
).show()

print(f"All outputs in: {tmp}")

conn.close()
