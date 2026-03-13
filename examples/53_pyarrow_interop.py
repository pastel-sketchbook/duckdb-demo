"""PyArrow interop — Arrow tables as virtual DuckDB tables, .to_arrow_table(), .fetchnumpy().

DuckDB speaks Apache Arrow natively. This means you can hand a PyArrow Table
to DuckDB and query it with SQL (zero-copy!), or pull DuckDB results out as
Arrow tables for downstream use with pandas, polars, or any Arrow-compatible
library.

Run with:
    uv run python examples/53_pyarrow_interop.py
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
import polars as pl

# -- PyArrow is the star of this example ---------------------------------------
import pyarrow as pa
import pyarrow.csv as pcsv  # Arrow's own CSV reader

# Point at the bundled sample data
DATA_DIR = Path(__file__).resolve().parent.parent / "src" / "duckdb_demo" / "data"
CSV_FILE = DATA_DIR / "sales.csv"

# =============================================================================
# 1. Create a PyArrow Table and query it directly with DuckDB SQL
# =============================================================================
print("=== 1. Query a PyArrow Table with DuckDB SQL ===")

# Build a PyArrow table entirely in Python — no CSV, no database.
# PyArrow is doing the work here: constructing columnar data in memory.
arrow_table = pa.table(
    {
        "city": ["Austin", "Denver", "Austin", "Denver", "Austin"],
        "year": [2023, 2023, 2024, 2024, 2024],
        "revenue": [100_000, 85_000, 120_000, 90_000, 115_000],
    }
)
print(f"PyArrow table: {arrow_table.num_rows} rows, {arrow_table.num_columns} columns")
print(f"Schema: {arrow_table.schema}\n")

# DuckDB can query the PyArrow variable by name — no import or registration
# needed. DuckDB automatically detects Python variables that are Arrow tables.
result = duckdb.sql(
    """
    SELECT
        city,
        SUM(revenue) AS total_revenue,
        COUNT(*)     AS num_records
    FROM arrow_table          -- references the Python variable directly!
    GROUP BY city
    ORDER BY total_revenue DESC
    """
)
result.show()

# =============================================================================
# 2. Convert DuckDB results to Arrow with .to_arrow_table()
# =============================================================================
print("=== 2. DuckDB results → Arrow Table with .to_arrow_table() ===")

# Run a query and call .to_arrow_table() to get a PyArrow Table back.
# DuckDB is doing the query; .to_arrow_table() converts the result to Arrow.
# (Note: .arrow() returns a RecordBatchReader; use .to_arrow_table() for a Table.)
arrow_result: pa.Table = duckdb.sql(
    """
    SELECT city, year, revenue
    FROM arrow_table
    WHERE revenue > 90000
    ORDER BY revenue DESC
    """
).to_arrow_table()

# Now we have a pure PyArrow table — DuckDB is no longer involved.
print(f"Type: {type(arrow_result).__module__}.{type(arrow_result).__name__}")
print(f"Rows: {arrow_result.num_rows}")
print(f"Columns: {arrow_result.column_names}")
print(arrow_result.to_pandas().to_string(index=False))  # quick peek via pandas
print()

# =============================================================================
# 3. .fetchnumpy() — get results as a dict of numpy arrays
# =============================================================================
print("=== 3. .fetchnumpy() — results as numpy arrays ===")

# .fetchnumpy() returns {column_name: numpy.ndarray}.
# Useful when you want raw arrays for matplotlib, scikit-learn, etc.
numpy_dict = duckdb.sql(
    """
    SELECT city, year, revenue
    FROM arrow_table
    ORDER BY year, city
    """
).fetchnumpy()

# Each value is a numpy array — great for numerical work.
for col_name, arr in numpy_dict.items():
    print(f"  {col_name:>10}: dtype={str(arr.dtype):<10} values={arr.tolist()}")
print()

# =============================================================================
# 4. Read the bundled sales.csv into Arrow, then query with DuckDB
# =============================================================================
print("=== 4. CSV → Arrow → DuckDB SQL ===")

# PyArrow reads the CSV — DuckDB is not involved in the file I/O here.
sales_arrow: pa.Table = pcsv.read_csv(CSV_FILE)
print(f"Loaded {CSV_FILE.name} via PyArrow: {sales_arrow.num_rows} rows")
print(f"Schema: {sales_arrow.schema}\n")

# Now hand the Arrow table to DuckDB for SQL analytics.
# DuckDB reads the Arrow table directly — no data copying.
print("Top products by revenue (DuckDB querying a PyArrow table):")
duckdb.sql(
    """
    SELECT
        product,
        SUM(quantity * price) AS revenue,
        SUM(quantity)         AS units_sold
    FROM sales_arrow
    GROUP BY product
    ORDER BY revenue DESC
    """
).show()

# =============================================================================
# 5. Arrow as the interchange format: DuckDB → Arrow → pandas / polars
# =============================================================================
print("=== 5. Interchange: DuckDB → Arrow → pandas / polars ===")

# Step 1: DuckDB produces an Arrow table from the query.
interchange: pa.Table = duckdb.sql(
    """
    SELECT
        customer,
        SUM(quantity * price) AS total_spent
    FROM sales_arrow
    GROUP BY customer
    ORDER BY total_spent DESC
    """
).to_arrow_table()
print(f"Arrow table from DuckDB: {interchange.num_rows} rows\n")

# Step 2a: Arrow → pandas (PyArrow does the conversion, not DuckDB).
pandas_df: pd.DataFrame = interchange.to_pandas()
print("As pandas DataFrame:")
print(pandas_df.to_string(index=False))
print()

# Step 2b: Arrow → polars (polars reads Arrow natively — zero-copy when possible).
polars_df: pl.DataFrame | pl.Series = pl.from_arrow(interchange)
print("As polars DataFrame:")
print(polars_df)
print()

# The pattern is:  DuckDB ──.to_arrow_table()──▶ PyArrow ──▶ any Arrow consumer
# This avoids format-specific conversion and keeps the data in columnar form.

# =============================================================================
# 6. Zero-copy advantage: DuckDB reads Arrow without copying data
# =============================================================================
print("=== 6. Zero-copy: DuckDB scans Arrow memory directly ===")

# Create a larger Arrow table to make the point clearer.
# PyArrow allocates the memory; DuckDB will read it in-place.
big_table = pa.table(
    {
        "id": pa.array(range(1_000_000), type=pa.int64()),
        "value": pa.array([float(i % 100) for i in range(1_000_000)], type=pa.float64()),
    }
)
nbytes = big_table.nbytes
print(f"Arrow table: {big_table.num_rows:,} rows, {nbytes:,} bytes in memory")

# DuckDB queries this without copying the underlying buffers.
# It maps the Arrow memory directly — this is what "zero-copy" means.
stats = duckdb.sql(
    """
    SELECT
        COUNT(*)    AS total_rows,
        AVG(value)  AS avg_value,
        MIN(value)  AS min_value,
        MAX(value)  AS max_value
    FROM big_table
    """
).to_arrow_table()

print("Query result (DuckDB read Arrow memory in-place, no copy):")
print(
    f"  rows={stats.column('total_rows')[0].as_py():,}"
    f"  avg={stats.column('avg_value')[0].as_py():.1f}"
    f"  min={stats.column('min_value')[0].as_py():.1f}"
    f"  max={stats.column('max_value')[0].as_py():.1f}"
)
print()

# =============================================================================
# 7. Key takeaways
# =============================================================================
print("=== 7. Key Takeaways ===")
print(
    """
  - Apache Arrow is a columnar memory format shared by many tools.
  - DuckDB reads PyArrow tables directly with zero-copy (no data duplication).
  - .to_arrow_table() → convert DuckDB results to a PyArrow Table.
  - .arrow()           → similar, but returns a RecordBatchReader (lazy/streaming).
  - .fetchnumpy()      → convert DuckDB results to a dict of numpy arrays.
  - Arrow tables work as virtual tables in DuckDB SQL (just use the variable name).
  - Arrow is the best interchange format: DuckDB → Arrow → pandas / polars / etc.
  - Reading a CSV with PyArrow and querying with DuckDB separates I/O from SQL.
"""
)
