"""Create tables and insert data with DuckDB.

Demonstrates how to:
  1. Create tables with explicit column types
  2. Insert rows with INSERT INTO and multi-row VALUES
  3. Use CREATE TABLE AS (CTAS) to build tables from queries
  4. Inspect table structure with DESCRIBE and SHOW TABLES

Run with:
    task run:example -- 05_create_and_insert.py
"""

from __future__ import annotations

import duckdb

# Use an in-memory database -- everything disappears when the script ends.
conn = duckdb.connect()

# -- 1. Create a table with explicit types ------------------------------------
print("=== Create a table with explicit column types ===")
conn.execute(
    """
    CREATE TABLE customers (
        id        INTEGER PRIMARY KEY,
        name      VARCHAR NOT NULL,
        email     VARCHAR,
        joined    DATE DEFAULT CURRENT_DATE,
        is_active BOOLEAN DEFAULT true
    )
    """
)
print("Table 'customers' created.")

# -- 2. Insert single and multiple rows --------------------------------------
print("\n=== Insert data ===")

# Single row
conn.execute(
    """
    INSERT INTO customers (id, name, email, joined)
    VALUES (1, 'Alice', 'alice@example.com', '2024-01-15')
    """
)

# Multiple rows in one INSERT
conn.execute(
    """
    INSERT INTO customers (id, name, email, joined) VALUES
        (2, 'Bob',     'bob@example.com',     '2024-02-20'),
        (3, 'Charlie', 'charlie@example.com', '2024-03-10'),
        (4, 'Diana',   NULL,                  '2024-04-05'),
        (5, 'Eve',     'eve@example.com',     '2024-05-01')
    """
)

# Parameterized insert -- safer than string interpolation
conn.execute(
    "INSERT INTO customers (id, name, email, joined) VALUES (?, ?, ?, ?)",
    [6, "Frank", "frank@example.com", "2024-06-15"],
)

row_count: int = conn.sql("SELECT COUNT(*) FROM customers").fetchone()[0]  # type: ignore[index]
print(f"Inserted {row_count} customers.")

conn.sql("SELECT * FROM customers ORDER BY id").show()

# -- 3. Inspect table structure -----------------------------------------------
print("=== DESCRIBE customers ===")
conn.sql("DESCRIBE customers").show()

print("=== SHOW TABLES ===")
conn.sql("SHOW TABLES").show()

# -- 4. CREATE TABLE AS (CTAS) ------------------------------------------------
# Build a new table from the result of a query -- great for materializing
# intermediate results or transformations.
print("=== CREATE TABLE AS -- active customers with email ===")
conn.execute(
    """
    CREATE TABLE active_customers AS
    SELECT id, name, email, joined
    FROM customers
    WHERE is_active = true
      AND email IS NOT NULL
    """
)

conn.sql("SELECT * FROM active_customers ORDER BY id").show()

# -- 5. Common DuckDB data types ---------------------------------------------
# DuckDB supports all standard SQL types plus a few extras.
print("=== DuckDB data types sampler ===")
conn.sql(
    """
    SELECT
        42              AS integer_val,
        3.14            AS double_val,
        'hello'         AS varchar_val,
        true            AS boolean_val,
        DATE '2024-06-15'          AS date_val,
        TIMESTAMP '2024-06-15 10:30:00' AS timestamp_val,
        INTERVAL '3 days'          AS interval_val,
        [1, 2, 3]      AS list_val,
        {'a': 1, 'b': 2}          AS struct_val
    """
).show()

# -- 6. INSERT from a query (INSERT INTO ... SELECT) --------------------------
print("=== INSERT INTO ... SELECT (copy rows between tables) ===")
conn.execute(
    """
    CREATE TABLE vip_customers (
        id   INTEGER,
        name VARCHAR
    )
    """
)

# Copy the first 3 customers into the VIP table
conn.execute(
    """
    INSERT INTO vip_customers
    SELECT id, name
    FROM customers
    WHERE id <= 3
    """
)

conn.sql("SELECT * FROM vip_customers ORDER BY id").show()

# -- Cleanup ------------------------------------------------------------------
conn.close()
print("Done!  All tables were in-memory and are now gone.")
