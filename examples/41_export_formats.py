"""Export to CSV & Parquet — writing query results to files.

Covers COPY ... TO, the Python .write_parquet() / .write_csv() methods,
format options (delimiters, headers, compression), and round-trip verification.

Run with:
    uv run python examples/41_export_formats.py
"""

from __future__ import annotations

import tempfile
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

# Use a temp directory so we don't litter the project with output files.
tmp = Path(tempfile.mkdtemp(prefix="duckdb_export_"))
print(f"Export directory: {tmp}\n")

# -- 1. COPY TO CSV — the SQL-based approach -----------------------------------
# COPY ... TO writes a query result directly to a file.
csv_out = tmp / "sales_export.csv"
conn.execute(
    f"""
    COPY (
        SELECT date, customer, product, quantity, quantity * price AS revenue
        FROM sales
        ORDER BY date
    ) TO '{csv_out}' (FORMAT CSV, HEADER TRUE)
    """
)
print(f"=== COPY TO CSV: wrote {csv_out.stat().st_size} bytes ===")

# Read it back to verify
print("Round-trip check:")
conn.sql(f"SELECT COUNT(*) AS rows FROM read_csv('{csv_out}')").show()

# -- 2. COPY TO CSV with options — delimiter, quoting, null string -------------
tsv_out = tmp / "sales_export.tsv"
conn.execute(
    f"""
    COPY sales TO '{tsv_out}' (
        FORMAT CSV,
        HEADER TRUE,
        DELIMITER '\t',
        NULL 'N/A'
    )
    """
)
print(f"=== COPY TO TSV (tab-delimited): wrote {tsv_out.stat().st_size} bytes ===")

# -- 3. COPY TO Parquet — columnar, compressed output --------------------------
parquet_out = tmp / "sales_export.parquet"
conn.execute(
    f"""
    COPY (
        SELECT date, customer, product, quantity, quantity * price AS revenue
        FROM sales
    ) TO '{parquet_out}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """
)
print(f"=== COPY TO Parquet (zstd): wrote {parquet_out.stat().st_size} bytes ===")

# Verify by reading metadata
print("Parquet metadata:")
conn.sql(
    f"""
    SELECT file_name, num_rows, num_row_groups
    FROM parquet_file_metadata('{parquet_out}')
    """
).show()

# -- 4. Python API: .write_csv() and .write_parquet() --------------------------
# The Python relation API provides .write_csv() and .write_parquet() methods
# directly on query results — no COPY statement needed.
py_csv = tmp / "from_python.csv"
py_parquet = tmp / "from_python.parquet"

# .write_csv() on a relation
conn.sql("SELECT * FROM sales WHERE product = 'Widget'").write_csv(str(py_csv))
print(f"\n=== .write_csv(): wrote {py_csv.stat().st_size} bytes ===")

# .write_parquet() on a relation
conn.sql("SELECT * FROM sales WHERE product = 'Gadget'").write_parquet(str(py_parquet))
print(f"=== .write_parquet(): wrote {py_parquet.stat().st_size} bytes ===")

# -- 5. COPY TO JSON — export as newline-delimited JSON -----------------------
json_out = tmp / "sales_export.jsonl"
conn.execute(
    f"""
    COPY (
        SELECT date, customer, product, quantity * price AS revenue
        FROM sales
        LIMIT 5
    ) TO '{json_out}' (FORMAT JSON, ARRAY false)
    """
)
print(f"\n=== COPY TO JSON (newline-delimited): wrote {json_out.stat().st_size} bytes ===")
print("First 3 lines:")
for i, line in enumerate(json_out.read_text().strip().split("\n")[:3]):
    print(f"  {line}")

# -- 6. Round-trip comparison: CSV vs Parquet file sizes -----------------------
print("\n=== File size comparison ===")
conn.sql(
    f"""
    SELECT
        '{csv_out.name}'     AS format,
        {csv_out.stat().st_size} AS bytes
    UNION ALL
    SELECT
        '{parquet_out.name}',
        {parquet_out.stat().st_size}
    UNION ALL
    SELECT
        '{json_out.name}',
        {json_out.stat().st_size}
    """
).show()

print(f"\nAll exports saved to: {tmp}")
print("(Temp directory — will be cleaned up by the OS)")

conn.close()
