"""Data quality checks — detect NULLs, outliers, duplicates, and constraint violations.

Demonstrates practical data quality patterns:
  1. SUMMARIZE for instant profiling
  2. NULL and completeness checks
  3. Uniqueness / duplicate detection
  4. Range and outlier checks
  5. Referential integrity validation
  6. Custom CHECK constraints

Run with:
    uv run python examples/82_data_quality.py
"""

from __future__ import annotations

from pathlib import Path

import duckdb

# -- Paths & connection -------------------------------------------------------
DATA_DIR = Path(__file__).resolve().parent.parent / "src" / "duckdb_demo" / "data"
CSV_FILE = DATA_DIR / "sales.csv"

conn = duckdb.connect()  # in-memory — no files left behind

# =============================================================================
# 1. Setup — build a deliberately messy dataset
# =============================================================================
print("=== 1. Setup — messy orders dataset ===")

# We create two reference tables (customers and products) and one "dirty"
# orders table that contains every common data-quality problem:
#   - NULL values in required fields
#   - Duplicate rows (same order_id repeated)
#   - Out-of-range values (negative quantity, future dates, extreme prices)
#   - Orphan foreign keys (customer_id / product_id that don't exist)

conn.execute(
    """
    CREATE TABLE customers AS
    SELECT * FROM (VALUES
        (1, 'Alice'),
        (2, 'Bob'),
        (3, 'Charlie'),
        (4, 'Diana')
    ) AS t(customer_id, name)
    """
)

conn.execute(
    """
    CREATE TABLE products AS
    SELECT * FROM (VALUES
        (101, 'Widget',  9.99),
        (102, 'Gadget', 24.99),
        (103, 'Gizmo',  49.99)
    ) AS t(product_id, name, unit_price)
    """
)

# The messy orders table — every row has at least one purpose.
conn.execute(
    """
    CREATE TABLE orders AS
    SELECT * FROM (VALUES
        -- Good rows
        (1,  1, 101, 10,  9.99, DATE '2024-01-05'),
        (2,  2, 102,  5, 24.99, DATE '2024-01-12'),
        (3,  3, 101,  7,  9.99, DATE '2024-02-03'),
        -- Duplicate order_id (id=4 appears twice)
        (4,  4, 103,  3, 49.99, DATE '2024-02-14'),
        (4,  4, 103,  3, 49.99, DATE '2024-02-14'),
        -- NULL customer_id (missing FK)
        (5, NULL, 102,  8, 24.99, DATE '2024-03-01'),
        -- NULL quantity and NULL price
        (6,  1, 101, NULL, 9.99, DATE '2024-03-15'),
        (7,  2, 103,  6, NULL,  DATE '2024-04-01'),
        -- Negative quantity (invalid)
        (8,  3, 102, -2, 24.99, DATE '2024-04-10'),
        -- Price way above expected range (data entry error)
        (9,  1, 101,  5, 9999.99, DATE '2024-04-18'),
        -- Future date (shouldn't exist yet)
        (10, 2, 103,  4, 49.99, DATE '2027-12-31'),
        -- Orphan foreign keys: customer 99 and product 999 don't exist
        (11, 99, 999,  2, 15.00, DATE '2024-05-01'),
        -- NULL order date
        (12, 3, 101,  1,  9.99, NULL)
    ) AS t(order_id, customer_id, product_id, quantity, price, order_date)
    """
)

# Also load the clean sales.csv for comparison later.
conn.execute(
    f"""
    CREATE TABLE clean_sales AS
    SELECT * FROM read_csv('{CSV_FILE}', auto_detect = true)
    """
)

print("Messy orders table:")
conn.sql("SELECT * FROM orders ORDER BY order_id").show()
print(f"Total rows: {conn.sql('SELECT COUNT(*) FROM orders').fetchone()[0]}\n")  # type: ignore[index]


# =============================================================================
# 2. SUMMARIZE — instant column-level profiling
# =============================================================================
print("=== 2. SUMMARIZE — instant profiling ===")

# DuckDB's SUMMARIZE gives you count, null_count (as null_percentage), min,
# max, approx_unique, and more — in one statement.  This is the fastest way
# to spot problems: look for high null_percentage or suspicious min/max.
conn.sql("SUMMARIZE orders").show()
print()


# =============================================================================
# 3. NULL / completeness checks
# =============================================================================
print("=== 3. NULL & Completeness Checks ===")

# For each column, count total rows, NULLs, and compute a completeness %.
# COLUMNS(*) with a lambda would be elegant but we keep it explicit for
# clarity — novice-friendly.
conn.sql(
    """
    SELECT
        COUNT(*)                                          AS total_rows,
        -- Each line: count NULLs then derive a percentage
        COUNT(*) - COUNT(order_id)                        AS null_order_id,
        COUNT(*) - COUNT(customer_id)                     AS null_customer_id,
        COUNT(*) - COUNT(product_id)                      AS null_product_id,
        COUNT(*) - COUNT(quantity)                         AS null_quantity,
        COUNT(*) - COUNT(price)                            AS null_price,
        COUNT(*) - COUNT(order_date)                       AS null_order_date
    FROM orders
    """
).show()

# Completeness as percentages — handy for dashboards.
print("Completeness percentages:")
conn.sql(
    """
    SELECT
        ROUND(100.0 * COUNT(order_id)    / COUNT(*), 1) AS order_id_pct,
        ROUND(100.0 * COUNT(customer_id) / COUNT(*), 1) AS customer_id_pct,
        ROUND(100.0 * COUNT(product_id)  / COUNT(*), 1) AS product_id_pct,
        ROUND(100.0 * COUNT(quantity)    / COUNT(*), 1) AS quantity_pct,
        ROUND(100.0 * COUNT(price)       / COUNT(*), 1) AS price_pct,
        ROUND(100.0 * COUNT(order_date)  / COUNT(*), 1) AS order_date_pct
    FROM orders
    """
).show()

# Show the actual rows that have NULLs — useful for manual review.
print("Rows with any NULL value:")
conn.sql(
    """
    SELECT *
    FROM orders
    WHERE customer_id IS NULL
       OR quantity    IS NULL
       OR price       IS NULL
       OR order_date  IS NULL
    ORDER BY order_id
    """
).show()
print()


# =============================================================================
# 4. Duplicate detection
# =============================================================================
print("=== 4. Duplicate Detection ===")

# Method A: GROUP BY + HAVING — find which order_ids appear more than once.
print("Duplicate order_ids (GROUP BY + HAVING):")
conn.sql(
    """
    SELECT order_id, COUNT(*) AS cnt
    FROM orders
    GROUP BY order_id
    HAVING COUNT(*) > 1
    ORDER BY order_id
    """
).show()

# Method B: ROW_NUMBER + QUALIFY — flag and keep only the first occurrence.
# QUALIFY filters on window function results (DuckDB extension to SQL).
print("Flagging duplicates with ROW_NUMBER + QUALIFY:")
conn.sql(
    """
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY order_id          -- deterministic tie-break
        ) AS row_num
    FROM orders
    QUALIFY row_num > 1                -- only the duplicate rows
    ORDER BY order_id
    """
).show()

# De-duplicated table: keep only the first occurrence of each order_id.
conn.execute(
    """
    CREATE TABLE orders_deduped AS
    SELECT * EXCLUDE (row_num) FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY order_id
                ORDER BY order_id
            ) AS row_num
        FROM orders
    )
    WHERE row_num = 1
    """
)
deduped_count: int = conn.sql(  # type: ignore[assignment]
    "SELECT COUNT(*) FROM orders_deduped"
).fetchone()[0]
print(f"After dedup: {deduped_count} rows (was 14)\n")


# =============================================================================
# 5. Range & outlier checks
# =============================================================================
print("=== 5. Range & Outlier Checks ===")

# Define business rules and flag violations with CASE expressions.
# Rules:
#   - quantity must be > 0
#   - price must be between 0 and 500 (anything above is suspect)
#   - order_date must not be in the future
print("Row-level quality flags:")
conn.sql(
    """
    SELECT
        order_id,
        quantity,
        price,
        order_date,
        -- Flag each type of violation
        CASE WHEN quantity IS NOT NULL AND quantity <= 0
             THEN 'FAIL: non-positive'
             ELSE 'OK'
        END AS qty_check,
        CASE WHEN price IS NOT NULL AND price > 500
             THEN 'FAIL: exceeds 500'
             WHEN price IS NOT NULL AND price < 0
             THEN 'FAIL: negative'
             ELSE 'OK'
        END AS price_check,
        CASE WHEN order_date IS NOT NULL AND order_date > CURRENT_DATE
             THEN 'FAIL: future date'
             ELSE 'OK'
        END AS date_check
    FROM orders_deduped
    ORDER BY order_id
    """
).show()

# Summary: how many rows fail each check?
print("Violation summary:")
conn.sql(
    """
    SELECT
        COUNT(*) FILTER (
            WHERE quantity IS NOT NULL AND quantity <= 0
        ) AS bad_quantity,
        COUNT(*) FILTER (
            WHERE price IS NOT NULL AND (price > 500 OR price < 0)
        ) AS bad_price,
        COUNT(*) FILTER (
            WHERE order_date IS NOT NULL AND order_date > CURRENT_DATE
        ) AS future_dates
    FROM orders_deduped
    """
).show()
print()


# =============================================================================
# 6. Referential integrity — orphan foreign keys
# =============================================================================
print("=== 6. Referential Integrity (orphan FK detection) ===")

# ANTI JOIN finds rows in orders that have no matching row in the reference
# table.  This is the standard pattern for detecting orphan foreign keys.

# Orphan customer_ids — orders referencing customers that don't exist.
print("Orphan customer_ids (ANTI JOIN):")
conn.sql(
    """
    SELECT o.order_id, o.customer_id
    FROM orders_deduped o
    ANTI JOIN customers c
        ON o.customer_id = c.customer_id
    WHERE o.customer_id IS NOT NULL   -- NULLs are missing, not orphans
    ORDER BY o.order_id
    """
).show()

# Orphan product_ids — orders referencing products that don't exist.
print("Orphan product_ids (ANTI JOIN):")
conn.sql(
    """
    SELECT o.order_id, o.product_id
    FROM orders_deduped o
    ANTI JOIN products p
        ON o.product_id = p.product_id
    WHERE o.product_id IS NOT NULL
    ORDER BY o.order_id
    """
).show()

# Bonus: LEFT JOIN approach — same result, different style.  The WHERE
# clause catches unmatched rows (NULL on the right side of the join).
print("Orphan product_ids (LEFT JOIN style, same result):")
conn.sql(
    """
    SELECT o.order_id, o.product_id
    FROM orders_deduped o
    LEFT JOIN products p
        ON o.product_id = p.product_id
    WHERE p.product_id IS NULL
      AND o.product_id IS NOT NULL
    ORDER BY o.order_id
    """
).show()
print()


# =============================================================================
# 7. Quality score — unified report
# =============================================================================
print("=== 7. Quality Score — unified report ===")

# Combine every check into one summary.  Each row is a quality dimension
# with a pass/fail count and a score (pass / total * 100).
conn.sql(
    """
    WITH base AS (
        SELECT COUNT(*) AS total FROM orders_deduped
    ),
    checks AS (
        SELECT
            -- 1) Completeness: no NULLs in required columns
            COUNT(*) FILTER (
                WHERE customer_id IS NOT NULL
                  AND product_id  IS NOT NULL
                  AND quantity    IS NOT NULL
                  AND price       IS NOT NULL
                  AND order_date  IS NOT NULL
            ) AS complete_rows,

            -- 2) No duplicate order_ids (already deduped, so all pass)
            COUNT(DISTINCT order_id)  AS unique_ids,
            COUNT(*)                  AS total_rows,

            -- 3) Quantity in valid range (> 0)
            COUNT(*) FILTER (
                WHERE quantity IS NULL OR quantity > 0
            ) AS valid_quantity,

            -- 4) Price in valid range (0 < price <= 500)
            COUNT(*) FILTER (
                WHERE price IS NULL OR (price > 0 AND price <= 500)
            ) AS valid_price,

            -- 5) Date not in future
            COUNT(*) FILTER (
                WHERE order_date IS NULL OR order_date <= CURRENT_DATE
            ) AS valid_date
        FROM orders_deduped
    )
    SELECT
        dimension,
        pass_count,
        fail_count,
        total,
        ROUND(100.0 * pass_count / total, 1) AS score_pct
    FROM (
        -- UNION ALL to stack each dimension as a separate row.
        SELECT 'Completeness'     AS dimension,
               c.complete_rows    AS pass_count,
               b.total - c.complete_rows AS fail_count,
               b.total            AS total
        FROM checks c, base b
        UNION ALL
        SELECT 'Uniqueness (order_id)',
               c.unique_ids,
               c.total_rows - c.unique_ids,
               c.total_rows
        FROM checks c
        UNION ALL
        SELECT 'Valid quantity',
               c.valid_quantity,
               b.total - c.valid_quantity,
               b.total
        FROM checks c, base b
        UNION ALL
        SELECT 'Valid price',
               c.valid_price,
               b.total - c.valid_price,
               b.total
        FROM checks c, base b
        UNION ALL
        SELECT 'Valid date',
               c.valid_date,
               b.total - c.valid_date,
               b.total
        FROM checks c, base b
        UNION ALL
        SELECT 'Referential (customer)',
               b.total - (
                   SELECT COUNT(*)
                   FROM orders_deduped o
                   ANTI JOIN customers cu
                       ON o.customer_id = cu.customer_id
                   WHERE o.customer_id IS NOT NULL
               ),
               (
                   SELECT COUNT(*)
                   FROM orders_deduped o
                   ANTI JOIN customers cu
                       ON o.customer_id = cu.customer_id
                   WHERE o.customer_id IS NOT NULL
               ),
               b.total
        FROM base b
        UNION ALL
        SELECT 'Referential (product)',
               b.total - (
                   SELECT COUNT(*)
                   FROM orders_deduped o
                   ANTI JOIN products pr
                       ON o.product_id = pr.product_id
                   WHERE o.product_id IS NOT NULL
               ),
               (
                   SELECT COUNT(*)
                   FROM orders_deduped o
                   ANTI JOIN products pr
                       ON o.product_id = pr.product_id
                   WHERE o.product_id IS NOT NULL
               ),
               b.total
        FROM base b
    )
    ORDER BY score_pct ASC   -- worst dimensions first
    """
).show()

# Overall quality score: average across all dimensions.
print("Overall quality score (average of all dimensions):")
conn.sql(
    """
    WITH base AS (
        SELECT COUNT(*) AS total FROM orders_deduped
    ),
    checks AS (
        SELECT
            COUNT(*) FILTER (
                WHERE customer_id IS NOT NULL
                  AND product_id  IS NOT NULL
                  AND quantity    IS NOT NULL
                  AND price       IS NOT NULL
                  AND order_date  IS NOT NULL
            ) AS complete_rows,
            COUNT(DISTINCT order_id) AS unique_ids,
            COUNT(*)                 AS total_rows,
            COUNT(*) FILTER (
                WHERE quantity IS NULL OR quantity > 0
            ) AS valid_quantity,
            COUNT(*) FILTER (
                WHERE price IS NULL OR (price > 0 AND price <= 500)
            ) AS valid_price,
            COUNT(*) FILTER (
                WHERE order_date IS NULL OR order_date <= CURRENT_DATE
            ) AS valid_date
        FROM orders_deduped
    ),
    orphan_counts AS (
        SELECT
            (SELECT COUNT(*) FROM orders_deduped o
             ANTI JOIN customers cu ON o.customer_id = cu.customer_id
             WHERE o.customer_id IS NOT NULL)  AS orphan_customers,
            (SELECT COUNT(*) FROM orders_deduped o
             ANTI JOIN products pr ON o.product_id = pr.product_id
             WHERE o.product_id IS NOT NULL)   AS orphan_products
    )
    SELECT
        ROUND(AVG(score), 1) AS overall_quality_pct
    FROM (
        SELECT 100.0 * c.complete_rows    / b.total     AS score
            FROM checks c, base b
        UNION ALL
        SELECT 100.0 * c.unique_ids       / c.total_rows FROM checks c
        UNION ALL
        SELECT 100.0 * c.valid_quantity   / b.total     FROM checks c, base b
        UNION ALL
        SELECT 100.0 * c.valid_price      / b.total     FROM checks c, base b
        UNION ALL
        SELECT 100.0 * c.valid_date       / b.total     FROM checks c, base b
        UNION ALL
        SELECT 100.0 * (b.total - oc.orphan_customers) / b.total
            FROM base b, orphan_counts oc
        UNION ALL
        SELECT 100.0 * (b.total - oc.orphan_products)  / b.total
            FROM base b, orphan_counts oc
    )
    """
).show()

# -- Comparison: the clean dataset has no issues --------------------------------
print("For comparison — clean sales.csv (no quality issues expected):")
conn.sql("SUMMARIZE clean_sales").show()

conn.close()
print("Done — all checks ran in-memory, no files created.")
