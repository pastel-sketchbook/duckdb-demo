"""Relational API — build queries with method chaining instead of SQL strings.

DuckDB's Relational API lets you construct queries programmatically using
Python methods like .filter(), .project(), .aggregate(), and .order().
Each method returns a new Relation, so you can chain them fluently.
This is especially useful for dynamic query building where the filters,
columns, or sort order are determined at Python at runtime.

Run with:
    uv run python examples/54_relational_api.py
"""

from __future__ import annotations

from pathlib import Path

import duckdb

# -- Setup ---------------------------------------------------------------------
DATA_DIR = Path(__file__).resolve().parent.parent / "src" / "duckdb_demo" / "data"
CSV_FILE = DATA_DIR / "sales.csv"

conn = duckdb.connect()

# -- 1. Loading data as a Relation --------------------------------------------
# A Relation is a lazy reference to a result set — nothing executes until you
# call .show(), .fetchall(), .df(), etc.  Think of it as a query builder.
print("=== 1. Load sales.csv into a Relation ===")

# read_csv returns a Relation directly — no need to CREATE TABLE first.
sales = conn.read_csv(str(CSV_FILE))

# .show() materialises and pretty-prints the result, just like duckdb.sql().show()
sales.show()

# You can also create a Relation from a SQL query:
#   sales = conn.sql(f"SELECT * FROM read_csv('{CSV_FILE}', auto_detect = true)")
# Both approaches produce the same Relation object.

# -- 2. .filter() — equivalent to WHERE ---------------------------------------
# .filter() accepts a SQL expression string that would go in a WHERE clause.
# It returns a *new* Relation (the original is unchanged — Relations are immutable).
print("=== 2. .filter() — orders where quantity >= 10 ===")

big_orders = sales.filter("quantity >= 10")
big_orders.show()

# You can chain multiple .filter() calls — they combine with AND logic.
print("=== 2b. Chained .filter() — Alice's big orders ===")
alice_big = sales.filter("customer = 'Alice'").filter("quantity >= 10")
alice_big.show()

# -- 3. .project() — equivalent to SELECT (column picking & expressions) ------
# .project() selects columns and can compute new ones using SQL expressions.
# Pass a comma-separated string of column names or expressions.
print("=== 3. .project() — select specific columns + computed column ===")

# Pick three columns and add a computed revenue column.
projected = sales.project("date, customer, product, quantity * price AS revenue")
projected.show()

# .project() is also how you rename columns:
print("=== 3b. .project() — renaming columns ===")
renamed = sales.project("customer AS buyer, product AS item, quantity AS qty")
renamed.show()

# -- 4. .aggregate() — equivalent to GROUP BY with aggregation -----------------
# .aggregate() takes two arguments:
#   1) The aggregation expressions (what you'd put in SELECT)
#   2) The grouping columns (what you'd put in GROUP BY)
print("=== 4. .aggregate() — total revenue by product ===")

revenue_by_product = sales.aggregate(
    "product, SUM(quantity * price) AS revenue, COUNT(*) AS order_count",
    "product",
)
revenue_by_product.show()

# Group by multiple columns — just comma-separate them.
print("=== 4b. .aggregate() — revenue by customer and product ===")
revenue_by_both = sales.aggregate(
    "customer, product, SUM(quantity * price) AS revenue",
    "customer, product",
)
revenue_by_both.show()

# -- 5. .order() — equivalent to ORDER BY -------------------------------------
# .order() takes a SQL ORDER BY expression string.
print("=== 5. .order() — products ordered by revenue descending ===")

sorted_products = revenue_by_product.order("revenue DESC")
sorted_products.show()

# Multiple sort keys work the same as SQL: separate with commas.
print("=== 5b. .order() — multi-column sort ===")
multi_sort = sales.order("customer ASC, date DESC")
multi_sort.show()

# -- 6. Method chaining — the real power of the Relational API -----------------
# Chain filter → project → aggregate → order in a single fluent expression.
# This builds the full query plan without executing until .show().
print("=== 6. Method chaining — full pipeline in one expression ===")

# Question: "For orders of 5+ units, what is each customer's total revenue?
#            Show the highest spenders first."
result = (
    sales.filter("quantity >= 5")  # WHERE
    .project("customer, quantity * price AS revenue")  # SELECT
    .aggregate("customer, SUM(revenue) AS total_revenue", "customer")  # GROUP BY
    .order("total_revenue DESC")  # ORDER BY
)
result.show()

# Each step returns a new Relation, so you can inspect intermediate results
# by assigning them to variables during debugging.

# -- 7. .limit() — equivalent to LIMIT ----------------------------------------
# .limit() caps the number of rows returned. Takes an int, not a string.
print("=== 7. .limit() — top 3 orders by revenue ===")

top_3 = (
    sales.project("date, customer, product, quantity * price AS revenue")
    .order("revenue DESC")
    .limit(3)
)
top_3.show()

# .limit() also accepts an optional offset parameter for pagination:
print("=== 7b. .limit() with offset — rows 4-6 (page 2) ===")
page_2 = (
    sales.project("date, customer, product, quantity * price AS revenue")
    .order("revenue DESC")
    .limit(3, offset=3)
)
page_2.show()

# -- 8. Side-by-side comparison — SQL string vs Relational API -----------------
# Both approaches produce identical results. Choose whichever is clearer
# for your use case: SQL for ad-hoc analysis, Relational API for dynamic
# query building in application code.
print("=== 8. Comparison — SQL string vs Relational API ===")

# The query: "Top 3 customers by total revenue for Widget orders"

# --- SQL string approach ---
print("--- SQL string ---")
conn.sql(
    f"""
    SELECT
        customer,
        SUM(quantity * price) AS total_revenue
    FROM read_csv('{CSV_FILE}', auto_detect = true)
    WHERE product = 'Widget'
    GROUP BY customer
    ORDER BY total_revenue DESC
    LIMIT 3
    """
).show()

# --- Relational API approach ---
print("--- Relational API ---")
(
    sales.filter("product = 'Widget'")
    .aggregate("customer, SUM(quantity * price) AS total_revenue", "customer")
    .order("total_revenue DESC")
    .limit(3)
).show()

# Both produce the same result.  The Relational API shines when the filter
# conditions, column selections, or sort order are built dynamically — e.g.
# from user input or configuration:
#
#   filters = ["quantity >= 5"]
#   if user_selected_product:
#       filters.append(f"product = '{user_selected_product}'")
#   rel = sales
#   for f in filters:
#       rel = rel.filter(f)
#   rel.aggregate(...).order(...).show()

conn.close()
