"""Reusable SQL queries and helper functions for the DuckDB demo.

Each function takes a DuckDB connection and returns a result relation.
This keeps SQL strings in one place so main.py stays readable.
"""

from __future__ import annotations

from pathlib import Path

import duckdb

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
# Resolve the `data/` directory relative to *this* file so the demo works
# regardless of the current working directory.
DATA_DIR: Path = Path(__file__).resolve().parent / "data"


# ---------------------------------------------------------------------------
# Table helpers
# ---------------------------------------------------------------------------
def create_sales_table(conn: duckdb.DuckDBPyConnection) -> duckdb.DuckDBPyRelation:
    """Create a 'sales' table from the bundled CSV file.

    DuckDB can read CSV files directly -- no pandas needed.
    """
    csv_path = DATA_DIR / "sales.csv"
    return conn.sql(
        f"""
        CREATE OR REPLACE TABLE sales AS
        SELECT * FROM read_csv('{csv_path}', auto_detect = true)
        """
    )


# ---------------------------------------------------------------------------
# Analytical queries
# ---------------------------------------------------------------------------
def total_revenue_by_product(conn: duckdb.DuckDBPyConnection) -> duckdb.DuckDBPyRelation:
    """Sum revenue per product, ordered highest-first."""
    return conn.sql(
        """
        SELECT
            product,
            SUM(quantity * price) AS total_revenue
        FROM sales
        GROUP BY product
        ORDER BY total_revenue DESC
        """
    )


def monthly_sales_summary(conn: duckdb.DuckDBPyConnection) -> duckdb.DuckDBPyRelation:
    """Aggregate sales by month using DuckDB date functions."""
    return conn.sql(
        """
        SELECT
            date_trunc('month', date)    AS month,
            COUNT(*)                     AS num_orders,
            SUM(quantity)                AS total_units,
            SUM(quantity * price)        AS total_revenue
        FROM sales
        GROUP BY month
        ORDER BY month
        """
    )


def top_customers(conn: duckdb.DuckDBPyConnection, *, limit: int = 5) -> duckdb.DuckDBPyRelation:
    """Return the top N customers by total spend."""
    return conn.sql(
        f"""
        SELECT
            customer,
            SUM(quantity * price) AS total_spend,
            COUNT(*)              AS num_orders
        FROM sales
        GROUP BY customer
        ORDER BY total_spend DESC
        LIMIT {limit}
        """
    )


def running_total_by_date(conn: duckdb.DuckDBPyConnection) -> duckdb.DuckDBPyRelation:
    """Demonstrate a window function: cumulative revenue over time."""
    return conn.sql(
        """
        SELECT
            date,
            product,
            quantity * price AS day_revenue,
            SUM(quantity * price) OVER (ORDER BY date) AS running_total
        FROM sales
        ORDER BY date
        """
    )
