"""Polars interop — query Polars DataFrames with DuckDB SQL and convert back.

Demonstrates how to:
  1. Query Polars DataFrames directly with DuckDB SQL (zero-copy)
  2. Convert DuckDB results to Polars with .pl()
  3. Use DuckDB SQL for aggregation/filtering on Polars data
  4. Integrate Polars LazyFrames with DuckDB
  5. Round-trip data between Polars and DuckDB tables
  6. Compare Polars-native operations with DuckDB SQL on the same data

Run with:
    uv run python examples/52_polars_interop.py
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import polars as pl

# -- Paths --------------------------------------------------------------------
DATA_DIR = Path(__file__).resolve().parent.parent / "src" / "duckdb_demo" / "data"
CSV_FILE = DATA_DIR / "sales.csv"

# -- 1. Query a Polars DataFrame with DuckDB SQL ------------------------------
# DuckDB can query any Polars DataFrame in the local scope by name — no
# registration or copying required.  The DataFrame is treated as a virtual
# table inside the SQL query.
print("=== 1. Query a Polars DataFrame with DuckDB SQL ===")

# Polars creates the DataFrame
df = pl.DataFrame(
    {
        "name": ["Alice", "Bob", "Charlie", "Diana"],
        "department": ["Engineering", "Marketing", "Engineering", "Marketing"],
        "salary": [95_000, 78_000, 102_000, 85_000],
    }
)
print("Polars DataFrame (created by Polars):")
print(df)

# DuckDB executes the SQL — it reads `df` directly from the Python scope
print("\nDuckDB SQL query on the Polars DataFrame:")
duckdb.sql(
    """
    SELECT
        department,
        COUNT(*)    AS headcount,
        AVG(salary) AS avg_salary
    FROM df
    GROUP BY department
    ORDER BY avg_salary DESC
    """
).show()

# -- 2. Convert DuckDB results to Polars with .pl() ---------------------------
# The .pl() method on a DuckDB relation returns a Polars DataFrame.
# This is the primary DuckDB → Polars conversion path.
print("=== 2. Convert DuckDB results to Polars with .pl() ===")

# DuckDB runs the query and returns a relation
result = duckdb.sql("SELECT name, salary, salary * 0.30 AS tax FROM df ORDER BY salary DESC")

# .pl() converts the DuckDB result into a Polars DataFrame
pl_result = result.pl()  # <-- DuckDB → Polars conversion

print(f"Type: {type(pl_result)}")  # polars.DataFrame
print(pl_result)

# -- 3. Query bundled sales.csv with DuckDB, convert to Polars ----------------
# DuckDB reads the CSV file directly (no Polars or pandas needed for reading).
# We then hand the result to Polars for further processing.
print("\n=== 3. Read CSV with DuckDB, convert to Polars ===")

# DuckDB reads and queries the CSV file
sales_pl = duckdb.sql(
    f"""
    SELECT
        date,
        customer,
        product,
        quantity,
        price,
        quantity * price AS revenue
    FROM read_csv('{CSV_FILE}', auto_detect = true)
    """
).pl()  # DuckDB result → Polars DataFrame

print(f"Type: {type(sales_pl)}")
print(f"Shape: {sales_pl.shape[0]} rows × {sales_pl.shape[1]} columns")
print(sales_pl.head(5))

# -- 4. DuckDB SQL on Polars DataFrames — aggregation & filtering -------------
# You can write complex SQL against Polars DataFrames.  DuckDB handles the
# heavy lifting (aggregation, window functions, etc.) while Polars holds the
# data.
print("\n=== 4. DuckDB SQL aggregation on a Polars DataFrame ===")

# DuckDB executes the aggregation — sales_pl is a Polars DataFrame
duckdb.sql(
    """
    SELECT
        product,
        SUM(revenue)                AS total_revenue,
        ROUND(AVG(revenue), 2)      AS avg_revenue,
        COUNT(*)                    AS order_count
    FROM sales_pl
    GROUP BY product
    ORDER BY total_revenue DESC
    """
).show()

print("DuckDB SQL filtering on a Polars DataFrame:")

# DuckDB executes the filter — again, sales_pl is pure Polars data
duckdb.sql(
    """
    SELECT customer, product, revenue
    FROM sales_pl
    WHERE revenue > 100
    ORDER BY revenue DESC
    """
).show()

# -- 5. LazyFrame integration -------------------------------------------------
# Polars LazyFrames represent a query plan that hasn't been executed yet.
# DuckDB can query LazyFrames just like eager DataFrames.
print("=== 5. LazyFrame integration ===")

# Polars creates a LazyFrame (nothing is computed yet)
lazy = pl.LazyFrame(
    {
        "city": ["NYC", "LA", "Chicago", "Houston", "Phoenix"],
        "population": [8_336_817, 3_979_576, 2_693_976, 2_304_580, 1_608_139],
        "state": ["NY", "CA", "IL", "TX", "AZ"],
    }
)
print(f"Type of lazy: {type(lazy)}")  # polars.LazyFrame

# DuckDB queries the LazyFrame directly — Polars materializes it on demand
print("\nDuckDB SQL on a Polars LazyFrame:")
duckdb.sql(
    """
    SELECT
        city,
        state,
        population,
        ROUND(100.0 * population / SUM(population) OVER (), 1) AS pct_of_total
    FROM lazy
    ORDER BY population DESC
    """
).show()

# -- 6. Round-trip: Polars → DuckDB table → query → Polars --------------------
# Sometimes you want to load Polars data into a persistent DuckDB table,
# run several queries, and then pull results back into Polars.
print("=== 6. Round-trip: Polars → DuckDB table → query → Polars ===")

conn = duckdb.connect()  # in-memory database

# Polars creates the data
orders = pl.DataFrame(
    {
        "order_id": [1, 2, 3, 4, 5],
        "customer": ["Alice", "Bob", "Alice", "Charlie", "Bob"],
        "amount": [120.50, 89.99, 45.00, 210.75, 67.30],
    }
)

# DuckDB ingests the Polars DataFrame into a persistent table
conn.execute("CREATE TABLE orders AS SELECT * FROM orders")

# DuckDB queries its own table (the data now lives inside DuckDB)
print("Query the DuckDB table:")
conn.sql(
    """
    SELECT
        customer,
        COUNT(*)       AS num_orders,
        SUM(amount)    AS total_spent
    FROM orders
    GROUP BY customer
    ORDER BY total_spent DESC
    """
).show()

# Pull the result back into Polars with .pl()
round_trip = conn.sql("SELECT * FROM orders ORDER BY order_id").pl()

print("Back in Polars after the round-trip:")
print(round_trip)
print(f"Type: {type(round_trip)}")

conn.close()

# -- 7. Compare: Polars native vs DuckDB SQL on same data ---------------------
# Both libraries can answer the same question.  This section shows the Polars
# way and the DuckDB way side by side so you can see the difference.
print("\n=== 7. Compare: Polars native vs DuckDB SQL ===")

# Question: "Total revenue per customer, sorted descending."

# --- Polars does this (pure Polars, no SQL) ---
print("Polars native approach:")
polars_answer = (
    sales_pl.group_by("customer")
    .agg(pl.col("revenue").sum().alias("total_revenue"))
    .sort("total_revenue", descending=True)
)
print(polars_answer)

# --- DuckDB executes the SQL (Polars holds the data) ---
print("\nDuckDB SQL approach (same result):")
duckdb.sql(
    """
    SELECT
        customer,
        SUM(revenue) AS total_revenue
    FROM sales_pl
    GROUP BY customer
    ORDER BY total_revenue DESC
    """
).show()

print("Both approaches produce the same answer.")
print("Use Polars when you prefer method-chaining; use DuckDB SQL when you")
print("prefer declarative SQL or need features like window functions and CTEs.")
