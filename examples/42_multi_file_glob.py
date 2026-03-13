"""Multi-file reads (globs) — query many files at once with wildcard patterns.

Covers read_csv('*.csv') glob patterns, the filename column for tracing
source files, union_by_name for schema-flexible merging, and list_files().

Run with:
    uv run python examples/42_multi_file_glob.py
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import duckdb

# -- Setup: create multiple CSV files to demonstrate glob reads ----------------
tmp = Path(tempfile.mkdtemp(prefix="duckdb_glob_"))

# Simulate monthly sales exports (3 files with the same schema)
for month, rows in [
    (
        "2024-01",
        [
            "date,customer,product,quantity,price",
            "2024-01-05,Alice,Widget,10,9.99",
            "2024-01-12,Bob,Gadget,5,24.99",
            "2024-01-20,Charlie,Widget,7,9.99",
        ],
    ),
    (
        "2024-02",
        [
            "date,customer,product,quantity,price",
            "2024-02-03,Alice,Gizmo,3,49.99",
            "2024-02-14,Diana,Gadget,12,24.99",
            "2024-02-28,Bob,Widget,20,9.99",
        ],
    ),
    (
        "2024-03",
        [
            "date,customer,product,quantity,price",
            "2024-03-05,Charlie,Gizmo,1,49.99",
            "2024-03-15,Alice,Gadget,8,24.99",
            "2024-03-22,Diana,Widget,15,9.99",
        ],
    ),
]:
    f = tmp / f"sales_{month}.csv"
    f.write_text("\n".join(rows) + "\n")

# Also create one file with an extra column (different schema)
extra = tmp / "sales_2024-04_extended.csv"
extra.write_text(
    "date,customer,product,quantity,price,channel\n"
    "2024-04-01,Bob,Gizmo,6,49.99,online\n"
    "2024-04-10,Charlie,Gadget,4,24.99,store\n"
)

print(f"Created sample files in: {tmp}\n")

conn = duckdb.connect()

# -- 1. Basic glob — read all CSVs matching a pattern -------------------------
# read_csv accepts glob patterns. *.csv matches every CSV in the directory.
print("=== Glob: read all sales_2024-0[1-3].csv files ===")
conn.sql(
    f"""
    SELECT *
    FROM read_csv('{tmp}/sales_2024-0[1-3].csv', auto_detect = true)
    ORDER BY date
    """
).show()

# -- 2. The filename column — trace which file each row came from --------------
# Setting filename = true adds a column showing the source file path.
print("=== filename column: trace each row to its source file ===")
conn.sql(
    f"""
    SELECT
        date,
        customer,
        product,
        -- Extract just the filename from the full path
        REGEXP_EXTRACT(filename, '[^/]+$') AS source_file
    FROM read_csv('{tmp}/sales_2024-0[1-3].csv', auto_detect = true, filename = true)
    ORDER BY date
    LIMIT 6
    """
).show()

# -- 3. Aggregate across multiple files ----------------------------------------
# DuckDB treats all globbed files as one virtual table — standard SQL works.
print("=== Aggregate across all files: monthly revenue ===")
conn.sql(
    f"""
    SELECT
        DATE_TRUNC('month', date) AS month,
        SUM(quantity * price)     AS revenue,
        COUNT(*)                  AS orders
    FROM read_csv('{tmp}/sales_2024-0[1-3].csv', auto_detect = true)
    GROUP BY month
    ORDER BY month
    """
).show()

# -- 4. union_by_name — merge files with different schemas ---------------------
# Standard glob fails when files have different columns. union_by_name
# matches columns by name and fills missing columns with NULL.
print("=== union_by_name: merge files with different schemas ===")
conn.sql(
    f"""
    SELECT
        date,
        customer,
        product,
        quantity * price AS revenue,
        channel  -- NULL for files without this column
    FROM read_csv('{tmp}/sales_*.csv', auto_detect = true, union_by_name = true)
    ORDER BY date
    """
).show()

# -- 5. Glob patterns — advanced matching -------------------------------------
# DuckDB supports standard glob syntax: *, ?, [ranges], **.
print("=== Glob patterns: only files with '01' or '03' in the name ===")
conn.sql(
    f"""
    SELECT COUNT(*) AS rows, MIN(date) AS first, MAX(date) AS last
    FROM read_csv('{tmp}/sales_2024-0[13].csv', auto_detect = true)
    """
).show()

# -- 6. Create a table from globbed files — materialise for reuse --------------
print("=== CREATE TABLE from glob (materialise all files) ===")
conn.execute(
    f"""
    CREATE TABLE all_sales AS
    SELECT *
    FROM read_csv('{tmp}/sales_2024-0[1-3].csv', auto_detect = true)
    """
)
conn.sql("SELECT COUNT(*) AS total_rows FROM all_sales").show()

# -- 7. list_files — see which files a glob matches ---------------------------
# Handy for debugging glob patterns before querying.
print("=== list_files: preview which files match ===")
conn.sql(
    f"""
    SELECT REGEXP_EXTRACT(file, '[^/]+$') AS filename
    FROM glob('{tmp}/sales_*.csv') t(file)
    ORDER BY filename
    """
).show()

print(f"\nAll sample files in: {tmp}")

conn.close()
