"""Pandas interop — query DataFrames with SQL, convert results to pandas.

DuckDB can query pandas DataFrames directly — no loading step required.
Results come back as DataFrames with .df() or .fetchdf(). When Arrow is
available, DuckDB uses zero-copy transfer for maximum performance.

Run with:
    uv run python examples/51_pandas_interop.py
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

DATA_DIR = Path(__file__).resolve().parent.parent / "src" / "duckdb_demo" / "data"
CSV_FILE = DATA_DIR / "sales.csv"


# =============================================================================
# 1. Query a pandas DataFrame directly with DuckDB SQL
# =============================================================================
# DuckDB can see local pandas DataFrames by name — no registration needed.
# Just use the variable name as a table reference in your SQL string.

print("=== 1. Query a pandas DataFrame directly with DuckDB SQL ===")

# pandas creates the DataFrame
df = pd.DataFrame(
    {
        "name": ["Alice", "Bob", "Charlie", "Diana"],
        "department": ["Engineering", "Sales", "Engineering", "Sales"],
        "salary": [95_000, 72_000, 105_000, 68_000],
    }
)
print("Source DataFrame (built by pandas):")
print(df)
print()

# DuckDB executes the SQL — it reads `df` directly, no copy or import step
result = duckdb.sql("SELECT name, salary FROM df WHERE salary > 70000 ORDER BY salary DESC")
print("DuckDB query result (filtered & sorted):")
result.show()


# =============================================================================
# 2. Convert DuckDB results to pandas with .df() and .fetchdf()
# =============================================================================
# .df() and .fetchdf() both return a pandas DataFrame.
# They are aliases — functionally identical.

print("=== 2. Convert DuckDB results to pandas with .df() / .fetchdf() ===")

# DuckDB runs the aggregation, then .df() converts the result to pandas
avg_salary = duckdb.sql(
    "SELECT department, AVG(salary) AS avg_salary FROM df GROUP BY department"
).df()  # <-- converts DuckDB result to a pandas DataFrame

# pandas owns this object now — we can use any pandas method
print("Type:", type(avg_salary))
print(avg_salary.to_string(index=False))
print()


# =============================================================================
# 3. CSV -> DuckDB -> pandas (real-world pipeline)
# =============================================================================
# DuckDB reads the CSV natively (fast, columnar scanning), then we hand
# the result to pandas for downstream Python work.

print("=== 3. CSV -> DuckDB -> pandas pipeline ===")

# DuckDB reads and aggregates the CSV file directly — pandas is not involved yet
sales_summary = duckdb.sql(
    f"""
    SELECT
        product,
        COUNT(*)              AS order_count,
        SUM(quantity * price) AS total_revenue
    FROM read_csv('{CSV_FILE}', auto_detect = true)
    GROUP BY product
    ORDER BY total_revenue DESC
    """
).df()  # <-- now it becomes a pandas DataFrame

# pandas owns the result — print it with pandas formatting
print("Sales summary (pandas DataFrame built from CSV via DuckDB):")
print(sales_summary.to_string(index=False))
print()


# =============================================================================
# 4. DuckDB SQL on a pandas DataFrame — aggregation & window functions
# =============================================================================
# Build a richer DataFrame in pandas, then let DuckDB handle the analytics.

print("=== 4. Aggregation & window functions on a pandas DataFrame ===")

# pandas creates the DataFrame
orders = pd.DataFrame(
    {
        "order_id": range(1, 9),
        "customer": ["Alice", "Bob", "Alice", "Charlie", "Bob", "Alice", "Charlie", "Bob"],
        "amount": [120.0, 85.0, 200.0, 55.0, 310.0, 95.0, 175.0, 60.0],
        "date": pd.to_datetime(
            [
                "2024-01-10",
                "2024-01-12",
                "2024-01-15",
                "2024-01-20",
                "2024-02-01",
                "2024-02-05",
                "2024-02-10",
                "2024-02-15",
            ]
        ),
    }
)

# DuckDB executes the aggregation — pandas just supplied the data
print("Total per customer (GROUP BY):")
duckdb.sql(
    """
    SELECT
        customer,
        COUNT(*)    AS orders,
        SUM(amount) AS total_spent
    FROM orders
    GROUP BY customer
    ORDER BY total_spent DESC
    """
).show()

# DuckDB executes the window function — running total per customer
print("Running total per customer (window function):")
duckdb.sql(
    """
    SELECT
        customer,
        date,
        amount,
        SUM(amount) OVER (
            PARTITION BY customer
            ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS running_total
    FROM orders
    ORDER BY customer, date
    """
).show()


# =============================================================================
# 5. .fetchdf() vs .df() — both return pandas DataFrames
# =============================================================================
# There is no practical difference. .fetchdf() is the older name; .df() is
# the modern shorthand. Both produce an identical pandas DataFrame.

print("=== 5. .fetchdf() vs .df() — both return pandas DataFrames ===")

query = "SELECT customer, SUM(amount) AS total FROM orders GROUP BY customer"

result_df = duckdb.sql(query).df()  # modern shorthand
result_fetchdf = duckdb.sql(query).fetchdf()  # older name, same result

print(f"  .df()      type: {type(result_df).__name__}, shape: {result_df.shape}")
print(f"  .fetchdf() type: {type(result_fetchdf).__name__}, shape: {result_fetchdf.shape}")

# Sort both by the same column so row order matches for comparison
a = result_df.sort_values("customer").reset_index(drop=True)
b = result_fetchdf.sort_values("customer").reset_index(drop=True)
print(f"  Values equal: {a.equals(b)}")
print()


# =============================================================================
# 6. Round-trip: pandas -> DuckDB table -> query -> pandas
# =============================================================================
# Sometimes you want to persist the DataFrame as a DuckDB table (e.g. to
# join multiple DataFrames or use indexes). Here's the full round-trip.

print("=== 6. Round-trip: pandas -> DuckDB table -> query -> pandas ===")

conn = duckdb.connect()  # in-memory database

# Step 1: pandas DataFrame -> DuckDB table
# DuckDB copies the data into its own columnar storage
conn.execute("CREATE TABLE orders_tbl AS SELECT * FROM orders")
print("Created DuckDB table 'orders_tbl' from pandas DataFrame")

# Step 2: Query the DuckDB table (pure SQL, pandas is not involved)
top_customers = conn.sql(
    """
    SELECT customer, SUM(amount) AS lifetime_value
    FROM orders_tbl
    GROUP BY customer
    HAVING SUM(amount) > 100
    ORDER BY lifetime_value DESC
    """
)

# Step 3: Convert the result back to pandas
top_df = top_customers.df()
print("Query result as pandas DataFrame:")
print(top_df.to_string(index=False))

conn.close()
print()


# =============================================================================
# 7. Performance note: zero-copy with Apache Arrow
# =============================================================================
# When DuckDB reads a pandas DataFrame, it must convert from row-oriented
# pandas/numpy storage to DuckDB's columnar format. This involves a copy.
#
# However, when Apache Arrow (pyarrow) is installed — and it is in this
# project — DuckDB can use Arrow as a shared columnar format. This enables
# *zero-copy* transfer in many cases:
#
#   - .arrow()     returns an Arrow Table (zero-copy from DuckDB internals)
#   - .df()        goes DuckDB -> Arrow -> pandas (Arrow leg is zero-copy)
#   - If you pass an Arrow-backed pandas DataFrame, DuckDB reads it without
#     copying the underlying buffers.
#
# Rule of thumb:
#   Small data  -> don't worry, .df() is fine.
#   Large data  -> prefer .arrow() or .fetchnumpy() if you can avoid pandas,
#                  or use Arrow-backed pandas dtypes (pd.ArrowDtype).

print("=== 7. Zero-copy performance note ===")

# DuckDB -> Arrow Table (zero-copy from DuckDB's columnar buffers)
# .arrow() returns a RecordBatchReader; call .read_all() to materialise as a Table
arrow_tbl = duckdb.sql("SELECT * FROM orders").arrow().read_all()
print(f"Arrow Table type: {type(arrow_tbl).__name__}, rows: {arrow_tbl.num_rows}")

# Arrow Table -> pandas (efficient, uses Arrow-backed conversion)
pandas_from_arrow = arrow_tbl.to_pandas()
print(f"pandas DataFrame shape: {pandas_from_arrow.shape}")
print()

print("Tip: For large datasets, staying in Arrow format avoids copies entirely.")
print("     Use .arrow() instead of .df() when pandas is not strictly needed.")
