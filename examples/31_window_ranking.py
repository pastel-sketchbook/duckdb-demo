"""Window functions — ranking: ROW_NUMBER, RANK, DENSE_RANK, NTILE.

Answers "who came first?", "what's the top N per group?", and "which
quartile does this row fall into?" — all without collapsing rows.

Run with:
    uv run python examples/31_window_ranking.py
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

# -- 1. ROW_NUMBER — assign a unique sequential number per partition -----------
# ROW_NUMBER() always gives consecutive integers (1, 2, 3 ...) with no gaps
# and no ties — even if two rows have the same ORDER BY value, they get
# different numbers (the tiebreaker is non-deterministic unless you add more
# ORDER BY columns).
print("=== ROW_NUMBER: rank each sale within its product ===")
conn.sql(
    """
    SELECT
        ROW_NUMBER() OVER (
            PARTITION BY product
            ORDER BY quantity * price DESC
        ) AS rn,
        product,
        customer,
        date,
        quantity * price AS revenue
    FROM sales
    ORDER BY product, rn
    """
).show()

# -- 2. Top-N per group — the classic ROW_NUMBER pattern ----------------------
# "Show me the single highest-revenue order for each product."
# This is probably the #1 use case for window functions in analytics.
print("=== Top-1 order per product (ROW_NUMBER + filter) ===")
conn.sql(
    """
    WITH ranked AS (
        SELECT
            ROW_NUMBER() OVER (
                PARTITION BY product
                ORDER BY quantity * price DESC
            ) AS rn,
            product,
            customer,
            date,
            quantity * price AS revenue
        FROM sales
    )
    SELECT product, customer, date, revenue
    FROM ranked
    WHERE rn = 1
    ORDER BY revenue DESC
    """
).show()

# -- 3. RANK — ties get the same rank, next rank skips -----------------------
# Unlike ROW_NUMBER, RANK gives the same number to tied values.
# After a tie, the next rank jumps (e.g., 1, 2, 2, 4 — no 3).
print("=== RANK: rank customers by total revenue (ties share a rank) ===")
conn.sql(
    """
    WITH customer_rev AS (
        SELECT customer, SUM(quantity * price) AS revenue
        FROM sales
        GROUP BY customer
    )
    SELECT
        RANK() OVER (ORDER BY revenue DESC) AS rank,
        customer,
        ROUND(revenue, 2) AS revenue
    FROM customer_rev
    ORDER BY rank
    """
).show()

# -- 4. DENSE_RANK — ties share a rank, no gaps in numbering -----------------
# Like RANK but the numbers stay dense: 1, 2, 2, 3 (not 1, 2, 2, 4).
print("=== DENSE_RANK: dense-rank products by order count ===")
conn.sql(
    """
    WITH product_counts AS (
        SELECT product, COUNT(*) AS order_count
        FROM sales
        GROUP BY product
    )
    SELECT
        DENSE_RANK() OVER (ORDER BY order_count DESC) AS dense_rank,
        RANK()       OVER (ORDER BY order_count DESC) AS rank,
        product,
        order_count
    FROM product_counts
    ORDER BY dense_rank
    """
).show()

# -- 5. Comparing ROW_NUMBER vs RANK vs DENSE_RANK side by side ---------------
# When values are unique all three produce the same result. Differences appear
# only when there are ties. Let's manufacture a tie to see the difference.
print("=== Side-by-side comparison (with manufactured ties) ===")
conn.sql(
    """
    WITH monthly AS (
        SELECT
            DATE_TRUNC('month', date) AS month,
            SUM(quantity * price)     AS revenue
        FROM sales
        GROUP BY month
    )
    SELECT
        month,
        revenue,
        ROW_NUMBER() OVER (ORDER BY revenue DESC) AS row_num,
        RANK()       OVER (ORDER BY revenue DESC) AS rank,
        DENSE_RANK() OVER (ORDER BY revenue DESC) AS dense_rank
    FROM monthly
    ORDER BY row_num
    """
).show()

# -- 6. NTILE — divide rows into N roughly equal buckets ----------------------
# NTILE(4) splits the result into quartiles (1, 2, 3, 4).
# Useful for percentile-based analysis ("top 25% of customers").
print("=== NTILE(4): assign orders to revenue quartiles ===")
conn.sql(
    """
    SELECT
        NTILE(4) OVER (ORDER BY quantity * price DESC) AS quartile,
        date,
        customer,
        product,
        quantity * price AS revenue
    FROM sales
    ORDER BY quartile, revenue DESC
    """
).show()

# -- 7. NTILE for segmentation — practical example ----------------------------
# "Label customers as Gold / Silver / Bronze based on lifetime value."
print("=== Customer segmentation with NTILE(3) ===")
conn.sql(
    """
    WITH customer_rev AS (
        SELECT customer, SUM(quantity * price) AS lifetime_value
        FROM sales
        GROUP BY customer
    ),
    segmented AS (
        SELECT
            customer,
            ROUND(lifetime_value, 2) AS lifetime_value,
            NTILE(3) OVER (ORDER BY lifetime_value DESC) AS tier
        FROM customer_rev
    )
    SELECT
        customer,
        lifetime_value,
        CASE tier
            WHEN 1 THEN 'Gold'
            WHEN 2 THEN 'Silver'
            WHEN 3 THEN 'Bronze'
        END AS segment
    FROM segmented
    ORDER BY tier, lifetime_value DESC
    """
).show()

conn.close()
