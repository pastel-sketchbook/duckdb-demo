"""Deduplication — detect, analyze, and remove duplicate records with DuckDB.

Demonstrates multiple deduplication strategies:
  1. Exact duplicate detection with GROUP BY + HAVING
  2. ROW_NUMBER() + QUALIFY for keeping "best" rows
  3. DISTINCT ON for simple dedup
  4. Fuzzy / near-duplicate detection
  5. Dedup with audit trail (keeping track of what was removed)

Run with:
    uv run python examples/83_deduplication.py
"""

from __future__ import annotations

from pathlib import Path  # noqa: F401 — required by project conventions

import duckdb

conn = duckdb.connect()

# =============================================================================
# 1. Setup — create a dataset with deliberate duplicates
# =============================================================================
# We model customer orders.  Some rows are exact copies (same in every column),
# and others are "near-duplicates" that differ only in case, whitespace, or
# timestamp — the kind of mess you see in real ETL pipelines.
print("=== 1. Setup: customer orders with duplicates ===")

conn.execute(
    """
    CREATE TABLE raw_orders (
        id         INTEGER,
        customer   VARCHAR,
        product    VARCHAR,
        quantity   INTEGER,
        price      DOUBLE,
        order_date DATE
    );

    INSERT INTO raw_orders VALUES
        -- Clean rows
        (1,  'Alice Smith',   'Widget A',  2,  19.99, '2024-03-01'),
        (2,  'Bob Jones',     'Widget B',  1,  29.99, '2024-03-01'),
        (3,  'Carol White',   'Widget A',  5,  19.99, '2024-03-02'),
        (4,  'Dave Brown',    'Widget C',  3,  49.99, '2024-03-02'),
        (5,  'Eve Davis',     'Widget B',  2,  29.99, '2024-03-03'),

        -- Exact duplicates of rows 1 and 2 (entire row is identical)
        (1,  'Alice Smith',   'Widget A',  2,  19.99, '2024-03-01'),
        (2,  'Bob Jones',     'Widget B',  1,  29.99, '2024-03-01'),
        (2,  'Bob Jones',     'Widget B',  1,  29.99, '2024-03-01'),

        -- Near-duplicates: same logical person, but casing/spacing differs
        (6,  'alice smith',   'Widget A',  1,  19.99, '2024-03-04'),
        (7,  ' Alice Smith ', 'widget a',  3,  19.99, '2024-03-05'),
        (8,  'CAROL WHITE',   'WIDGET A',  5,  19.99, '2024-03-02'),

        -- Late-arriving correction for Dave: different id, same business key
        (9,  'Dave Brown',    'Widget C',  3,  44.99, '2024-03-02'),

        -- Additional clean rows
        (10, 'Frank Green',   'Widget D',  1,  99.99, '2024-03-04'),
        (11, 'Grace Lee',     'Widget A',  4,  19.99, '2024-03-05');
    """
)

conn.sql("SELECT * FROM raw_orders ORDER BY id, customer").show()
print(f"Total rows: {conn.sql('SELECT COUNT(*) FROM raw_orders').fetchone()[0]}\n")  # type: ignore[index]

# =============================================================================
# 2. Detect exact duplicates — GROUP BY all columns + HAVING COUNT(*) > 1
# =============================================================================
# The simplest duplicate check: if every single column matches, the rows are
# exact copies.  GROUP BY all columns and keep only groups with more than one
# row.  This tells you *which* rows are duplicated and *how many* copies exist.
print("=== 2. Detect exact duplicates (GROUP BY + HAVING) ===")

conn.sql(
    """
    SELECT
        id, customer, product, quantity, price, order_date,
        COUNT(*) AS copy_count
    FROM raw_orders
    GROUP BY id, customer, product, quantity, price, order_date
    HAVING COUNT(*) > 1
    ORDER BY id
    """
).show()
# Alice (id=1) has 2 copies, Bob (id=2) has 3 copies.

# Quick way to count how many rows are duplicates vs unique:
conn.sql(
    """
    WITH dup_counts AS (
        SELECT COUNT(*) AS cnt
        FROM raw_orders
        GROUP BY id, customer, product, quantity, price, order_date
    )
    SELECT
        SUM(cnt)           AS total_rows,
        COUNT(*)           AS unique_rows,
        SUM(cnt) - COUNT(*) AS duplicate_rows
    FROM dup_counts
    """
).show()

# =============================================================================
# 3. ROW_NUMBER() + QUALIFY — keep the "best" row per business key
# =============================================================================
# In real data you often want to keep one row per business key (e.g.,
# customer + product + date).  ROW_NUMBER() + QUALIFY lets you pick a winner
# with a tiebreaker — here we keep the row with the highest id (latest entry).
#
# QUALIFY is DuckDB's elegant alternative to wrapping in a CTE + WHERE.  It
# filters rows based on window function results, similar to HAVING for GROUP BY.
print("=== 3. ROW_NUMBER() + QUALIFY: keep latest row per business key ===")

conn.sql(
    """
    SELECT
        id,
        customer,
        product,
        quantity,
        price,
        order_date,
        ROW_NUMBER() OVER (
            PARTITION BY customer, product, order_date
            ORDER BY id DESC          -- highest id = most recent entry wins
        ) AS rn
    FROM raw_orders
    QUALIFY rn = 1
    ORDER BY id
    """
).show()
# Notice that for Dave Brown / Widget C / 2024-03-02, id=9 wins over id=4
# because it has the higher id (the later correction).

# =============================================================================
# 4. DISTINCT ON — DuckDB's simpler syntax for the same pattern
# =============================================================================
# DISTINCT ON (cols) ORDER BY ... keeps the first row per group according to
# the ORDER BY.  It produces the same result as ROW_NUMBER() = 1 but with
# less syntax.  Side-by-side comparison:
print("=== 4. DISTINCT ON: simpler syntax for single-row-per-group ===")

conn.sql(
    """
    SELECT DISTINCT ON (customer, product, order_date)
        id,
        customer,
        product,
        quantity,
        price,
        order_date
    FROM raw_orders
    ORDER BY customer, product, order_date, id DESC   -- last id wins
    """
).show()

print("-- Verification: both approaches produce the same row count --")
rn_count = conn.sql(
    """
    SELECT COUNT(*) FROM (
        SELECT *, ROW_NUMBER() OVER (
            PARTITION BY customer, product, order_date
            ORDER BY id DESC
        ) AS rn
        FROM raw_orders
        QUALIFY rn = 1
    )
    """
).fetchone()[0]  # type: ignore[index]

distinct_on_count = conn.sql(
    """
    SELECT COUNT(*) FROM (
        SELECT DISTINCT ON (customer, product, order_date) *
        FROM raw_orders
        ORDER BY customer, product, order_date, id DESC
    )
    """
).fetchone()[0]  # type: ignore[index]

print(f"  ROW_NUMBER + QUALIFY : {rn_count} rows")
print(f"  DISTINCT ON         : {distinct_on_count} rows")
print(f"  Match               : {rn_count == distinct_on_count}\n")

# =============================================================================
# 5. Fuzzy / near-duplicate detection — normalization + similarity
# =============================================================================
# Exact dedup misses rows that differ only in casing, whitespace, or minor
# typos.  We first normalize (LOWER + TRIM) and then group by the cleaned
# values.  For true fuzzy matching, DuckDB provides jaro_winkler_similarity.
print("=== 5. Fuzzy / near-duplicate detection ===")

# Step A: Normalize and find groups that collapse into the same key
print("-- 5a. Normalization: LOWER(TRIM(customer)) reveals hidden duplicates --")
conn.sql(
    """
    SELECT
        LOWER(TRIM(customer)) AS norm_customer,
        LOWER(TRIM(product))  AS norm_product,
        COUNT(*)              AS occurrences,
        ARRAY_AGG(id ORDER BY id) AS ids,
        ARRAY_AGG(customer ORDER BY id) AS original_names
    FROM raw_orders
    GROUP BY norm_customer, norm_product
    HAVING COUNT(*) > 1
    ORDER BY occurrences DESC
    """
).show()
# "Alice Smith", "alice smith", and " Alice Smith " all collapse to the same
# normalized form, revealing them as near-duplicates.

# Step B: Jaro-Winkler similarity for pairs of customer names
# This finds names that are "close enough" even if normalization doesn't
# make them identical (e.g., typos like "Alic Smith" vs "Alice Smith").
print("-- 5b. Jaro-Winkler similarity: find close-but-not-identical names --")
conn.sql(
    """
    WITH unique_names AS (
        SELECT DISTINCT LOWER(TRIM(customer)) AS name
        FROM raw_orders
    )
    SELECT
        a.name                                  AS name_a,
        b.name                                  AS name_b,
        ROUND(jaro_winkler_similarity(a.name, b.name), 4) AS similarity
    FROM unique_names a
    CROSS JOIN unique_names b
    WHERE a.name < b.name                       -- avoid self-pairs and dups
      AND jaro_winkler_similarity(a.name, b.name) > 0.85
    ORDER BY similarity DESC
    """
).show()
# Pairs with similarity > 0.85 are likely the same person.

# Step C: Dedup using normalized keys — keep highest id per normalized group
print("-- 5c. Dedup with normalized keys (keep latest per normalized group) --")
conn.sql(
    """
    SELECT DISTINCT ON (
        LOWER(TRIM(customer)),
        LOWER(TRIM(product)),
        order_date
    )
        id,
        customer,
        product,
        quantity,
        price,
        order_date
    FROM raw_orders
    ORDER BY
        LOWER(TRIM(customer)),
        LOWER(TRIM(product)),
        order_date,
        id DESC                  -- latest entry wins
    """
).show()

# =============================================================================
# 6. Dedup with audit trail — clean table + rejected table
# =============================================================================
# In production you rarely just delete rows.  Instead, you create a "clean"
# table and a "rejected" table so you can audit what was removed and why.
print("=== 6. Dedup with audit trail ===")

# First, rank every row within its normalized business-key group.
# Rows with rn = 1 are kept; the rest are rejected.
conn.execute(
    """
    CREATE TABLE ranked_orders AS
    SELECT
        *,
        LOWER(TRIM(customer)) AS norm_customer,
        LOWER(TRIM(product))  AS norm_product,
        ROW_NUMBER() OVER (
            PARTITION BY LOWER(TRIM(customer)),
                         LOWER(TRIM(product)),
                         order_date
            ORDER BY id DESC              -- latest id wins
        ) AS rn
    FROM raw_orders
    """
)

# Clean table: only the winners (rn = 1)
conn.execute(
    """
    CREATE TABLE clean_orders AS
    SELECT id, customer, product, quantity, price, order_date
    FROM ranked_orders
    WHERE rn = 1
    """
)

# Rejected table: the losers (rn > 1) with a reason column
conn.execute(
    """
    CREATE TABLE rejected_orders AS
    SELECT
        id,
        customer,
        product,
        quantity,
        price,
        order_date,
        norm_customer,
        norm_product,
        rn          AS duplicate_rank,
        CASE
            -- Exact duplicate: every column matches the kept row
            WHEN EXISTS (
                SELECT 1 FROM ranked_orders k
                WHERE k.rn = 1
                  AND k.norm_customer = ranked_orders.norm_customer
                  AND k.norm_product  = ranked_orders.norm_product
                  AND k.order_date    = ranked_orders.order_date
                  AND k.id            = ranked_orders.id
                  AND k.quantity      = ranked_orders.quantity
                  AND k.price         = ranked_orders.price
            ) THEN 'exact_duplicate'
            -- Near-duplicate: same normalized key, different raw values
            ELSE 'near_duplicate'
        END AS rejection_reason
    FROM ranked_orders
    WHERE rn > 1
    """
)

print("-- Clean orders (kept) --")
conn.sql("SELECT * FROM clean_orders ORDER BY id").show()

print("-- Rejected orders (removed) --")
conn.sql(
    """
    SELECT id, customer, product, quantity, price, order_date,
           duplicate_rank, rejection_reason
    FROM rejected_orders
    ORDER BY norm_customer, norm_product, order_date, duplicate_rank
    """
).show()

# Summary of the audit
print("-- Audit summary --")
conn.sql(
    """
    SELECT
        rejection_reason,
        COUNT(*) AS rejected_count
    FROM rejected_orders
    GROUP BY rejection_reason
    ORDER BY rejected_count DESC
    """
).show()

# =============================================================================
# 7. Before/after comparison — verify no business-key data loss
# =============================================================================
# The most important check after dedup: did we lose any *distinct* business
# entities?  The number of unique (normalized customer, product, date) combos
# should be identical before and after.
print("=== 7. Before / after comparison ===")

before_stats = conn.sql(
    """
    SELECT
        COUNT(*)                                        AS total_rows,
        COUNT(DISTINCT (LOWER(TRIM(customer))
            || '|' || LOWER(TRIM(product))
            || '|' || order_date::VARCHAR))             AS unique_biz_keys
    FROM raw_orders
    """
).fetchone()
assert before_stats is not None

after_stats = conn.sql(
    """
    SELECT
        COUNT(*)                                        AS total_rows,
        COUNT(DISTINCT (LOWER(TRIM(customer))
            || '|' || LOWER(TRIM(product))
            || '|' || order_date::VARCHAR))             AS unique_biz_keys
    FROM clean_orders
    """
).fetchone()
assert after_stats is not None

rejected_count = conn.sql("SELECT COUNT(*) FROM rejected_orders").fetchone()[0]  # type: ignore[index]

print(f"  Before dedup : {before_stats[0]} rows, {before_stats[1]} unique business keys")
print(f"  After dedup  : {after_stats[0]} rows, {after_stats[1]} unique business keys")
print(f"  Rejected     : {rejected_count} rows")
print(
    f"  Rows check   : {before_stats[0]} = {after_stats[0]} + {rejected_count}"
    f"  -> {'PASS' if before_stats[0] == after_stats[0] + rejected_count else 'FAIL'}"
)
print(
    f"  Key check    : {before_stats[1]} = {after_stats[1]}"
    f"  -> {'PASS' if before_stats[1] == after_stats[1] else 'FAIL'}"
)

print()
print("-- Final clean dataset --")
conn.sql("SELECT * FROM clean_orders ORDER BY id").show()

conn.close()
