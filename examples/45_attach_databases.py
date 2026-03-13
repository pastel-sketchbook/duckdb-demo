"""ATTACH & multi-database — query across multiple databases.

Covers ATTACH/DETACH for connecting to additional DuckDB files,
USE for switching default databases, and cross-database queries.

Run with:
    uv run python examples/45_attach_databases.py
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import duckdb

# -- Setup: create two DuckDB database files to demonstrate ATTACH -------------
DATA_DIR = Path(__file__).resolve().parent.parent / "src" / "duckdb_demo" / "data"
CSV_FILE = DATA_DIR / "sales.csv"
tmp = Path(tempfile.mkdtemp(prefix="duckdb_attach_"))

# Database 1: sales data
sales_db = tmp / "sales.duckdb"
with duckdb.connect(str(sales_db)) as c:
    c.execute(
        f"""
        CREATE TABLE sales AS
        SELECT * FROM read_csv('{CSV_FILE}', auto_detect = true)
        """
    )
    c.execute("SELECT COUNT(*) FROM sales")
    print(f"Created {sales_db.name}: {c.fetchone()[0]} rows in 'sales' table")  # type: ignore[index]

# Database 2: product catalog
catalog_db = tmp / "catalog.duckdb"
with duckdb.connect(str(catalog_db)) as c:
    c.execute(
        """
        CREATE TABLE products AS
        SELECT * FROM (VALUES
            ('Widget',  'Hardware',     9.99),
            ('Gadget',  'Electronics', 24.99),
            ('Gizmo',   'Electronics', 49.99)
        ) AS t(name, category, list_price)
        """
    )
    c.execute("SELECT COUNT(*) FROM products")
    print(f"Created {catalog_db.name}: {c.fetchone()[0]} rows in 'products' table")  # type: ignore[index]

print()

# -- 1. ATTACH — connect to another database file -----------------------------
# Start with an in-memory database, then attach the two files.
conn = duckdb.connect()

conn.execute(f"ATTACH '{sales_db}' AS sales_db (READ_ONLY)")
conn.execute(f"ATTACH '{catalog_db}' AS catalog_db (READ_ONLY)")

print("=== ATTACH: connected databases ===")
conn.sql("SELECT database_name, path, type FROM duckdb_databases()").show()

# -- 2. Query attached databases using fully-qualified names -------------------
# Use database.schema.table notation to reference tables in attached databases.
print("=== Query attached database: sales_db.main.sales ===")
conn.sql(
    """
    SELECT date, customer, product, quantity
    FROM sales_db.main.sales
    ORDER BY date
    LIMIT 5
    """
).show()

print("=== Query attached database: catalog_db.main.products ===")
conn.sql("SELECT * FROM catalog_db.main.products").show()

# -- 3. Cross-database JOIN — combine data from different databases ------------
# This is the key power feature: join tables that live in different .duckdb files.
print("=== Cross-database JOIN: sales enriched with catalog ===")
conn.sql(
    """
    SELECT
        s.date,
        s.customer,
        s.product,
        p.category,
        s.quantity,
        s.quantity * p.list_price AS revenue
    FROM sales_db.main.sales s
    INNER JOIN catalog_db.main.products p
        ON s.product = p.name
    ORDER BY revenue DESC
    LIMIT 8
    """
).show()

# -- 4. USE — change the default database -------------------------------------
# After USE, you can reference tables without the database prefix.
conn.execute("USE sales_db")

print("=== USE sales_db: now 'sales' resolves without prefix ===")
conn.sql("SELECT COUNT(*) AS sales_count FROM sales").show()

# Switch back to the default in-memory database
conn.execute("USE memory")

# -- 5. Create local tables from attached data ---------------------------------
# Materialise remote data into your in-memory session for faster repeated queries.
print("=== Materialise: CTAS from attached database into memory ===")
conn.execute(
    """
    CREATE TABLE local_enriched AS
    SELECT
        s.date,
        s.customer,
        s.product,
        p.category,
        s.quantity * s.price AS revenue
    FROM sales_db.main.sales s
    LEFT JOIN catalog_db.main.products p ON s.product = p.name
    """
)
conn.sql(
    """
    SELECT category, SUM(revenue) AS total_revenue
    FROM local_enriched
    GROUP BY category
    ORDER BY total_revenue DESC
    """
).show()

# -- 6. DETACH — disconnect an attached database -------------------------------
conn.execute("DETACH sales_db")
conn.execute("DETACH catalog_db")

print("=== DETACH: databases disconnected ===")
conn.sql("SELECT database_name FROM duckdb_databases()").show()

# The local_enriched table still exists (it was copied into memory)
print("=== Local table survives DETACH ===")
conn.sql("SELECT COUNT(*) AS rows FROM local_enriched").show()

# -- 7. Key patterns -----------------------------------------------------------
print("=== Key ATTACH patterns ===")
print("  ATTACH 'file.duckdb' AS db_name              -- read/write")
print("  ATTACH 'file.duckdb' AS db_name (READ_ONLY)  -- read-only")
print("  USE db_name                                   -- set default")
print("  SELECT * FROM db_name.schema.table            -- fully qualified")
print("  DETACH db_name                                -- disconnect")

print(f"\nDatabase files in: {tmp}")

conn.close()
