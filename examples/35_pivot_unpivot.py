"""PIVOT & UNPIVOT — reshaping data between long and wide formats.

Covers DuckDB's PIVOT ... ON ... USING syntax, UNPIVOT for reversing
the transformation, and practical reporting use cases.

Run with:
    uv run python examples/35_pivot_unpivot.py
"""

from __future__ import annotations

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

# Build a monthly summary as the base for pivoting.
conn.execute(
    """
    CREATE TABLE monthly_product AS
    SELECT
        DATE_TRUNC('month', date) AS month,
        product,
        SUM(quantity * price)     AS revenue
    FROM sales
    GROUP BY month, product
    ORDER BY month, product
    """
)

# -- 1. Before: the "long" format (one row per month × product) ---------------
print("=== Long format (before PIVOT) ===")
conn.sql("SELECT * FROM monthly_product ORDER BY month, product").show()

# -- 2. PIVOT — turn row values into columns -----------------------------------
# PIVOT syntax: PIVOT <table> ON <column> USING <aggregate>(<value>)
# This turns each distinct product into its own column.
print("=== PIVOT: products as columns, months as rows ===")
conn.sql(
    """
    PIVOT monthly_product
    ON product
    USING SUM(revenue)
    GROUP BY month
    ORDER BY month
    """
).show()

# -- 3. PIVOT with multiple aggregates ----------------------------------------
# You can apply more than one aggregate function. Each gets its own set of columns.
print("=== PIVOT with SUM and COUNT ===")
conn.sql(
    """
    PIVOT (
        SELECT
            DATE_TRUNC('month', date) AS month,
            product,
            quantity * price AS revenue
        FROM sales
    )
    ON product
    USING SUM(revenue), COUNT(*)
    GROUP BY month
    ORDER BY month
    """
).show()

# -- 4. PIVOT with IN — restrict which values become columns -------------------
# By default PIVOT creates a column for every distinct value. Use IN to pick
# only the ones you want — useful when there are many categories.
print("=== PIVOT with IN: only Widget and Gadget ===")
conn.sql(
    """
    PIVOT monthly_product
    ON product IN ('Widget', 'Gadget')
    USING SUM(revenue)
    GROUP BY month
    ORDER BY month
    """
).show()

# -- 5. UNPIVOT — turn columns back into rows ---------------------------------
# UNPIVOT is the inverse of PIVOT: it melts wide-format columns into rows.
# First create a wide table, then unpivot it.
print("=== Wide table (created from PIVOT) ===")
conn.execute(
    """
    CREATE TABLE wide_revenue AS
    PIVOT monthly_product
    ON product
    USING SUM(revenue)
    GROUP BY month
    """
)
conn.sql("SELECT * FROM wide_revenue ORDER BY month").show()

print("=== UNPIVOT: melt columns back to rows ===")
conn.sql(
    """
    UNPIVOT wide_revenue
    ON Gadget, Gizmo, Widget
    INTO
        NAME product
        VALUE revenue
    ORDER BY month, product
    """
).show()

# -- 6. UNPIVOT with column selection patterns ---------------------------------
# Use COLUMNS(*) to unpivot all non-GROUP-BY columns, or EXCLUDE to skip some.
print("=== UNPIVOT with COLUMNS — unpivot all product columns ===")
conn.sql(
    """
    UNPIVOT wide_revenue
    ON COLUMNS(* EXCLUDE month)
    INTO
        NAME product
        VALUE revenue
    ORDER BY month, product
    """
).show()

# -- 7. Practical reporting: customer × product matrix -------------------------
# Real-world use case: create a cross-tab of customers vs products.
print("=== Customer × Product revenue matrix ===")
conn.sql(
    """
    PIVOT (
        SELECT customer, product, SUM(quantity * price) AS revenue
        FROM sales
        GROUP BY customer, product
    )
    ON product
    USING SUM(revenue)
    GROUP BY customer
    ORDER BY customer
    """
).show()

conn.close()
