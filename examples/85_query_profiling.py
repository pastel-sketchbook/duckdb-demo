"""Query profiling — understand and optimize DuckDB query plans.

Demonstrates DuckDB's profiling and optimization tools:
  1. EXPLAIN for logical and physical query plans
  2. EXPLAIN ANALYZE for actual runtime statistics
  3. PRAGMA enable_profiling for detailed output
  4. Reading and interpreting query plan operators
  5. Comparing query strategies for the same result

Run with:
    uv run python examples/85_query_profiling.py
"""

from __future__ import annotations

import json
import os
import tempfile as _tempfile
from pathlib import Path

import duckdb

# -- Setup ---------------------------------------------------------------------
DATA_DIR = Path(__file__).resolve().parent.parent / "src" / "duckdb_demo" / "data"
CSV_FILE = DATA_DIR / "sales.csv"

conn = duckdb.connect()

# =============================================================================
# 1. Setup — load sales.csv and generate a larger synthetic dataset
# =============================================================================
# We load the real sales.csv for familiarity, then create a 10K+ row synthetic
# dataset so that profiling numbers (rows, timings) are meaningful.
print("=== 1. Setup — Load Data & Generate Synthetic Dataset ===")

# Load the small real dataset from CSV.
conn.execute(
    f"""
    CREATE TABLE sales AS
    SELECT * FROM read_csv('{CSV_FILE}', auto_detect = true)
    """
)
sales_row = conn.sql("SELECT count(*) FROM sales").fetchone()
print(f"  Loaded sales.csv: {sales_row[0] if sales_row else 0} rows")

# Generate a 10,000-row synthetic orders table using generate_series + random().
# This gives us enough data to see meaningful numbers in EXPLAIN ANALYZE output.
# - generate_series(1, 10000) produces row IDs
# - We derive random customers, products, quantities, and prices from the ID
conn.execute(
    """
    CREATE TABLE orders AS
    SELECT
        id                                              AS order_id,
        -- 50 distinct customers: 'customer_01' .. 'customer_50'
        'customer_' || lpad(cast((id % 50) + 1 AS varchar), 2, '0')
                                                        AS customer,
        -- 5 product categories chosen by modulo
        CASE (id % 5)
            WHEN 0 THEN 'Widget'
            WHEN 1 THEN 'Gadget'
            WHEN 2 THEN 'Gizmo'
            WHEN 3 THEN 'Doohickey'
            ELSE        'Thingamajig'
        END                                             AS product,
        -- quantity between 1 and 20
        (hash(id) % 20 + 1)::INTEGER                   AS quantity,
        -- price between 5.00 and 99.99
        round(5.0 + (hash(id * 7) % 9500) / 100.0, 2) AS price,
        -- dates spanning 2024
        DATE '2024-01-01' + cast(id % 365 AS integer)   AS order_date
    FROM generate_series(1, 10000) AS t(id)
    """
)

# A second table for JOIN demos — one row per customer with a region.
conn.execute(
    """
    CREATE TABLE customers AS
    SELECT DISTINCT
        customer,
        CASE (hash(customer) % 4)
            WHEN 0 THEN 'North'
            WHEN 1 THEN 'South'
            WHEN 2 THEN 'East'
            ELSE        'West'
        END AS region
    FROM orders
    """
)

row_count: int = conn.sql("SELECT count(*) FROM orders").fetchone()[0]  # type: ignore[index]
cust_count: int = conn.sql("SELECT count(*) FROM customers").fetchone()[0]  # type: ignore[index]
print(f"  Generated orders table: {row_count} rows")
print(f"  Generated customers table: {cust_count} rows\n")


# =============================================================================
# Helper to print a query plan with a label
# =============================================================================
def show_plan(label: str, plan_result: duckdb.DuckDBPyRelation) -> None:
    """Print a plan result with a descriptive label."""
    print(f"  [{label}]")
    plan_result.show()


# =============================================================================
# 2. EXPLAIN — reading logical and physical query plans
# =============================================================================
# EXPLAIN shows what DuckDB *will* do, without executing the query.
# The default output is the physical (optimized) plan.  Key operators to know:
#   - SEQ_SCAN     : sequential scan of a table (reads every row)
#   - FILTER       : applies a WHERE condition to discard rows
#   - PROJECTION   : selects/computes the output columns
#   - HASH_GROUP_BY: aggregation using a hash table
#   - HASH_JOIN    : join using a hash table
print("=== 2. EXPLAIN — Logical & Physical Plans ===")

# Simple filtered query — notice FILTER on top of SEQ_SCAN.
print("\n  Query: SELECT * FROM orders WHERE product = 'Widget' AND quantity > 10")
show_plan(
    "Physical plan",
    conn.sql(
        """
        EXPLAIN
        SELECT order_id, customer, product, quantity
        FROM orders
        WHERE product = 'Widget' AND quantity > 10
        """
    ),
)
# Reading the plan bottom-up:
#   1. SEQ_SCAN on 'orders' — reads all 10K rows from the table
#   2. FILTER — applies "product = 'Widget' AND quantity > 10"
#   3. PROJECTION — keeps only the four requested columns
# Tip: always read plans bottom-up — data flows from leaves to root.

# =============================================================================
# 3. EXPLAIN ANALYZE — actual runtime statistics
# =============================================================================
# EXPLAIN ANALYZE actually *runs* the query and annotates each operator with:
#   - The number of rows that passed through it
#   - The wall-clock time spent in that operator
# This is invaluable for finding bottlenecks in slow queries.
print("=== 3. EXPLAIN ANALYZE — Runtime Statistics ===")

print("\n  Same filtered query, now with runtime stats:")
show_plan(
    "EXPLAIN ANALYZE",
    conn.sql(
        """
        EXPLAIN ANALYZE
        SELECT order_id, customer, product, quantity
        FROM orders
        WHERE product = 'Widget' AND quantity > 10
        """
    ),
)
# Compare with plain EXPLAIN:
#   - EXPLAIN shows the plan structure only (no execution)
#   - EXPLAIN ANALYZE executes and reports real row counts + timing
# Look for operators that process many rows but output few — those are your
# bottlenecks. A FILTER that scans 10K rows but keeps 100 suggests an index
# or partition strategy might help.

print("  Aggregation query:")
show_plan(
    "EXPLAIN ANALYZE — COUNT by product",
    conn.sql(
        """
        EXPLAIN ANALYZE
        SELECT product, count(*) AS cnt, sum(quantity * price) AS revenue
        FROM orders
        GROUP BY product
        ORDER BY revenue DESC
        """
    ),
)

# =============================================================================
# 4. Profiling a JOIN — see the join strategy
# =============================================================================
# When you join two tables, DuckDB chooses a join algorithm.  HASH_JOIN is
# the most common: it builds a hash table from the smaller (build) side and
# probes it with rows from the larger (probe) side.
print("=== 4. Profiling a JOIN ===")

print("\n  Query: orders JOIN customers ON customer, grouped by region")
show_plan(
    "EXPLAIN ANALYZE — JOIN",
    conn.sql(
        """
        EXPLAIN ANALYZE
        SELECT
            c.region,
            count(*)              AS order_count,
            sum(o.quantity * o.price) AS revenue
        FROM orders o
        JOIN customers c ON o.customer = c.customer
        GROUP BY c.region
        ORDER BY revenue DESC
        """
    ),
)
# In the plan you should see:
#   - HASH_JOIN: the customers table (50 rows) is the build side,
#     orders (10K rows) is the probe side.  DuckDB picks the smaller
#     table for the hash table — this is efficient.
#   - HASH_GROUP_BY: aggregates revenue per region after the join.
# If you see NESTED_LOOP_JOIN instead of HASH_JOIN, it usually means
# one side is very small or there's a complex join condition.

# =============================================================================
# 5. Profiling aggregations — vectorized GROUP BY
# =============================================================================
# DuckDB uses a vectorized execution engine that processes data in chunks
# (vectors of ~2048 rows).  GROUP BY uses a hash table internally.
# Let's profile a multi-level aggregation.
print("=== 5. Profiling Aggregations ===")

print("\n  Multi-level aggregation: monthly revenue by product")
show_plan(
    "EXPLAIN ANALYZE — monthly aggregation",
    conn.sql(
        """
        EXPLAIN ANALYZE
        SELECT
            date_trunc('month', order_date) AS month,
            product,
            count(*)                        AS orders,
            sum(quantity)                   AS total_qty,
            round(avg(price), 2)            AS avg_price,
            sum(quantity * price)           AS revenue
        FROM orders
        GROUP BY month, product
        ORDER BY month, revenue DESC
        """
    ),
)
# Key observations:
#   - HASH_GROUP_BY processes all 10K rows and outputs (12 months × 5 products)
#     = ~60 groups.  The ratio of input-to-output rows shows the aggregation
#     compression factor.
#   - ORDER_BY sorts only the ~60 result rows, not the original 10K.
#   - DuckDB's vectorized engine processes these in batches, so even 10K rows
#     is handled in just a few vector iterations.

# =============================================================================
# 6. Comparing strategies — subquery vs JOIN
# =============================================================================
# A common question: are two equivalent SQL formulations equally fast?
# Let's compare a correlated subquery vs a JOIN for finding each customer's
# total revenue alongside their most-purchased product.
print("=== 6. Comparing Query Strategies ===")

# Strategy A: Subquery to find the top product per customer
print("\n  Strategy A — Subquery with window function:")
show_plan(
    "EXPLAIN ANALYZE — subquery strategy",
    conn.sql(
        """
        EXPLAIN ANALYZE
        SELECT customer, product, revenue
        FROM (
            SELECT
                customer,
                product,
                sum(quantity * price)                          AS revenue,
                row_number() OVER (PARTITION BY customer
                                   ORDER BY sum(quantity * price) DESC) AS rn
            FROM orders
            GROUP BY customer, product
        ) sub
        WHERE rn = 1
        ORDER BY revenue DESC
        """
    ),
)

# Strategy B: JOIN with a CTE that pre-computes the max revenue
print("  Strategy B — CTE + JOIN:")
show_plan(
    "EXPLAIN ANALYZE — CTE + JOIN strategy",
    conn.sql(
        """
        EXPLAIN ANALYZE
        WITH customer_product_revenue AS (
            SELECT
                customer,
                product,
                sum(quantity * price) AS revenue
            FROM orders
            GROUP BY customer, product
        ),
        max_revenue AS (
            SELECT customer, max(revenue) AS max_rev
            FROM customer_product_revenue
            GROUP BY customer
        )
        SELECT cpr.customer, cpr.product, cpr.revenue
        FROM customer_product_revenue cpr
        JOIN max_revenue mr
            ON cpr.customer = mr.customer AND cpr.revenue = mr.max_rev
        ORDER BY cpr.revenue DESC
        """
    ),
)
# Compare the two plans:
#   - Strategy A uses a WINDOW operator + FILTER — DuckDB's optimizer is
#     very good at window functions, so this is typically efficient.
#   - Strategy B uses two HASH_GROUP_BY operators + HASH_JOIN — more
#     operators but each is simple.
# In DuckDB, the window function approach (A) is usually slightly more
# efficient because the optimizer can combine the aggregation and ranking
# into a single pass over the data.

# =============================================================================
# 7. PRAGMA enable_profiling — JSON profiling output
# =============================================================================
# For programmatic analysis of query performance, DuckDB can output profiling
# data as JSON.  This is useful for dashboards, logging, or automated
# performance testing.
#
# PRAGMA enable_profiling = 'json' tells DuckDB to record detailed profiling
# data for every subsequent query.  PRAGMA profiling_output directs that JSON
# to a file.  We use a temp file and read it back.
print("=== 7. PRAGMA enable_profiling — JSON Output ===")

# Create a temp file to capture the JSON profiling output.
_fd, _tmp_str = _tempfile.mkstemp(suffix=".json")
os.close(_fd)  # close the file descriptor; DuckDB will write to the path
profile_path = Path(_tmp_str)

conn.execute("PRAGMA enable_profiling = 'json'")
conn.execute(f"PRAGMA profiling_output = '{profile_path}'")

# Run a representative query — the profiling data is written to the file.
# We use .sql().fetchall() so the query fully executes and profiling flushes.
conn.sql(
    """
    SELECT
        c.region,
        o.product,
        count(*)                  AS orders,
        sum(o.quantity * o.price) AS revenue
    FROM orders o
    JOIN customers c ON o.customer = c.customer
    GROUP BY c.region, o.product
    ORDER BY c.region, revenue DESC
    """
).fetchall()

# Read and parse the JSON profiling output.
profile: dict = json.loads(profile_path.read_text())
profile_path.unlink(missing_ok=True)  # clean up temp file

# The JSON contains top-level metrics and a tree of operator nodes.
print("  JSON profiling output (top-level keys):")
top_keys = [k for k in profile if k != "children"]
print(f"    {top_keys}")

# Print overall query timing.
latency = profile.get("latency", 0.0)
print(f"    Total query latency: {latency:.6f}s")
print(f"    Rows returned: {profile.get('rows_returned', 'n/a')}")
print(f"    Cumulative rows scanned: {profile.get('cumulative_rows_scanned', 'n/a')}")


# Walk through the operator tree and print each node's name and timing.
def print_operator_tree(node: dict, depth: int = 0) -> None:
    """Recursively print each operator in the profiling tree."""
    indent = "    " + "  " * depth
    op_type = node.get("operator_type", "unknown")
    op_name = node.get("operator_name", op_type)
    timing = node.get("operator_timing", "n/a")
    cardinality = node.get("operator_cardinality", "n/a")

    # Format timing nicely if it's a float
    if isinstance(timing, float):
        timing_str = f"{timing:.6f}s"
    else:
        timing_str = str(timing)

    print(f"{indent}{op_name} ({op_type})  rows={cardinality}  time={timing_str}")

    # Recurse into child operators
    for child in node.get("children", []):
        print_operator_tree(child, depth + 1)


print("\n  Operator tree with timing breakdown:")
for child in profile.get("children", []):
    print_operator_tree(child)

# Disable profiling to avoid overhead on subsequent queries.
conn.execute("PRAGMA disable_profiling")
print()

# -- Summary -------------------------------------------------------------------
print("=== Summary ===")
print(
    """
Key takeaways:
  - EXPLAIN shows the plan WITHOUT executing — use it to understand structure
  - EXPLAIN ANALYZE executes and shows real row counts + timing per operator
  - Read plans bottom-up: data flows from leaf nodes (scans) to root (result)
  - Key operators to recognize:
      SEQ_SCAN       — full table scan (reads every row)
      FILTER         — applies WHERE conditions
      PROJECTION     — selects/computes output columns
      HASH_JOIN      — join via hash table (smaller side = build)
      HASH_GROUP_BY  — aggregation via hash table
      WINDOW         — window function computation
      ORDER_BY       — sorting
  - Compare strategies with EXPLAIN ANALYZE to find the most efficient approach
  - PRAGMA enable_profiling = 'json' gives machine-readable timing breakdowns
  - DuckDB's vectorized engine processes data in batches (~2048 rows at a time)
"""
)

conn.close()
