"""Entry-point for the DuckDB demo.

Run with:
    uv run python -m duckdb_demo.main

The demo walks through several common analytics tasks using DuckDB's
in-process SQL engine -- no database server required.
"""

from __future__ import annotations

import duckdb

from duckdb_demo.queries import (
    create_sales_table,
    monthly_sales_summary,
    running_total_by_date,
    top_customers,
    total_revenue_by_product,
)


def _banner(title: str) -> None:
    """Print a section header so output is easy to scan."""
    width = 60
    print()
    print("=" * width)
    print(f"  {title}")
    print("=" * width)


def main() -> None:
    """Run the full demo against an in-memory DuckDB database."""
    # ── 1. Connect (in-memory -- nothing to install or configure) ────────
    conn = duckdb.connect()

    # ── 2. Load the sample CSV into a table ──────────────────────────────
    _banner("Loading sales.csv into DuckDB")
    create_sales_table(conn)
    print("Created table 'sales'. Preview:")
    conn.sql("SELECT * FROM sales LIMIT 5").show()

    # ── 3. Total revenue by product ──────────────────────────────────────
    _banner("Total Revenue by Product")
    total_revenue_by_product(conn).show()

    # ── 4. Monthly sales summary ─────────────────────────────────────────
    _banner("Monthly Sales Summary")
    monthly_sales_summary(conn).show()

    # ── 5. Top customers ─────────────────────────────────────────────────
    _banner("Top 5 Customers by Spend")
    top_customers(conn, limit=5).show()

    # ── 6. Running total (window function) ───────────────────────────────
    _banner("Running Revenue Total (Window Function)")
    running_total_by_date(conn).show()

    # ── Done ─────────────────────────────────────────────────────────────
    conn.close()
    print("\nDemo complete. Explore the examples/ directory for more.")


if __name__ == "__main__":
    main()
