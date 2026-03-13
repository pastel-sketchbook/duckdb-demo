"""Set operations — combining result sets with UNION, INTERSECT, and EXCEPT.

Covers UNION (deduplicated), UNION ALL (keep duplicates), UNION BY NAME,
INTERSECT, and EXCEPT for comparing and merging query results.

Run with:
    uv run python examples/26_set_operations.py
"""

from __future__ import annotations

import duckdb

conn = duckdb.connect()

# -- Setup: create two regional sales tables with overlapping data -------------
# Using inline data so the set operations are crystal clear.
conn.execute(
    """
    CREATE TABLE sales_east AS
    SELECT * FROM (VALUES
        ('2024-01-10', 'Alice',   'Widget', 10, 9.99),
        ('2024-01-15', 'Bob',     'Gadget', 5,  24.99),
        ('2024-02-01', 'Charlie', 'Gizmo',  3,  49.99),
        ('2024-02-20', 'Diana',   'Widget', 8,  9.99)
    ) AS t(date, customer, product, quantity, price)
    """
)

conn.execute(
    """
    CREATE TABLE sales_west AS
    SELECT * FROM (VALUES
        ('2024-01-10', 'Alice',   'Widget', 10, 9.99),
        ('2024-01-22', 'Eve',     'Gadget', 7,  24.99),
        ('2024-02-01', 'Charlie', 'Gizmo',  3,  49.99),
        ('2024-03-05', 'Frank',   'Gizmo',  2,  49.99)
    ) AS t(date, customer, product, quantity, price)
    """
)

print("=== Sales East ===")
conn.sql("SELECT * FROM sales_east").show()
print("=== Sales West ===")
conn.sql("SELECT * FROM sales_west").show()

# -- 1. UNION ALL — stack all rows, keep duplicates ---------------------------
# UNION ALL simply concatenates the two result sets. If a row appears in both
# tables (like Alice's Widget order), it appears twice in the output.
print("=== UNION ALL: all rows from both regions (duplicates kept) ===")
conn.sql(
    """
    SELECT *, 'east' AS region FROM sales_east
    UNION ALL
    SELECT *, 'west' AS region FROM sales_west
    ORDER BY date, customer
    """
).show()

# -- 2. UNION — stack rows and remove exact duplicates ------------------------
# UNION deduplicates — identical rows appear only once. This is more expensive
# than UNION ALL because DuckDB must compare every row.
# Note: the 'region' tag would make rows unique, so we omit it here.
print("=== UNION: deduplicated combination ===")
conn.sql(
    """
    SELECT * FROM sales_east
    UNION
    SELECT * FROM sales_west
    ORDER BY date, customer
    """
).show()

# -- 3. INTERSECT — rows that appear in BOTH result sets ----------------------
# Returns only the rows that are identical in both queries.
# Useful for finding overlap ("which transactions were recorded in both systems?").
print("=== INTERSECT: rows in both East AND West ===")
conn.sql(
    """
    SELECT * FROM sales_east
    INTERSECT
    SELECT * FROM sales_west
    ORDER BY date
    """
).show()

# -- 4. EXCEPT — rows in the first set but NOT in the second ------------------
# Returns rows from the left query that don't appear in the right query.
# Order matters: A EXCEPT B != B EXCEPT A.
print("=== EXCEPT: rows only in East (not in West) ===")
conn.sql(
    """
    SELECT * FROM sales_east
    EXCEPT
    SELECT * FROM sales_west
    ORDER BY date
    """
).show()

print("=== EXCEPT (reversed): rows only in West (not in East) ===")
conn.sql(
    """
    SELECT * FROM sales_west
    EXCEPT
    SELECT * FROM sales_east
    ORDER BY date
    """
).show()

# -- 5. UNION BY NAME — match columns by name, not position -------------------
# Standard UNION matches columns by *position* (1st col with 1st col).
# DuckDB's UNION BY NAME matches by *column name* — extra columns become NULL.
# This is invaluable when combining tables with slightly different schemas.
print("=== UNION BY NAME: tables with different column sets ===")

conn.execute(
    """
    CREATE TABLE online_sales AS
    SELECT * FROM (VALUES
        ('2024-03-01', 'Gina', 'Widget', 5, 9.99, 'web')
    ) AS t(date, customer, product, quantity, price, channel)
    """
)

conn.sql(
    """
    SELECT date, customer, product, quantity, price FROM sales_east
    UNION BY NAME
    SELECT * FROM online_sales
    ORDER BY date
    """
).show()

# -- 6. Set operations with aggregation — practical pattern --------------------
# Combine set operations with GROUP BY to answer business questions.
# "Which products are sold in both regions, and what's each region's total?"
print("=== Products sold in both regions with per-region totals ===")
conn.sql(
    """
    WITH both_regions AS (
        SELECT product FROM sales_east
        INTERSECT
        SELECT product FROM sales_west
    )
    SELECT
        'east' AS region,
        e.product,
        SUM(e.quantity * e.price) AS revenue
    FROM sales_east e
    WHERE e.product IN (SELECT product FROM both_regions)
    GROUP BY e.product

    UNION ALL

    SELECT
        'west' AS region,
        w.product,
        SUM(w.quantity * w.price) AS revenue
    FROM sales_west w
    WHERE w.product IN (SELECT product FROM both_regions)
    GROUP BY w.product

    ORDER BY product, region
    """
).show()

# -- 7. INTERSECT ALL / EXCEPT ALL — preserve duplicate counts ----------------
# The ALL variants keep track of how many times a row appears.
# INTERSECT ALL: if row appears 2x in A and 3x in B, result has it 2x (minimum).
# EXCEPT ALL: if row appears 3x in A and 1x in B, result has it 2x (difference).
print("=== INTERSECT ALL vs INTERSECT (with duplicate rows) ===")
conn.sql(
    """
    -- Add duplicate row to East
    SELECT 'INTERSECT ALL count' AS label,
           COUNT(*) AS rows
    FROM (
        SELECT * FROM sales_east
        INTERSECT ALL
        SELECT * FROM sales_west
    )
    UNION ALL
    SELECT 'INTERSECT count',
           COUNT(*)
    FROM (
        SELECT * FROM sales_east
        INTERSECT
        SELECT * FROM sales_west
    )
    """
).show()

conn.close()
