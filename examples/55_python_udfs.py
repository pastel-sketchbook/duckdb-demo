"""Python UDFs — extend DuckDB SQL with custom Python functions.

Register Python functions as User-Defined Functions (UDFs) so they can be
called directly from SQL queries.  Covers scalar UDFs, type mapping,
NULL handling, error handling, and practical patterns using sales.csv.

Run with:
    uv run python examples/55_python_udfs.py
"""

from __future__ import annotations

import re
from pathlib import Path

import duckdb
from duckdb import func, sqltypes

# -- Setup ---------------------------------------------------------------------
DATA_DIR = Path(__file__).resolve().parent.parent / "src" / "duckdb_demo" / "data"
CSV_FILE = DATA_DIR / "sales.csv"

conn = duckdb.connect()

# Load sales data into a table for later sections
conn.execute(
    f"""
    CREATE TABLE sales AS
    SELECT * FROM read_csv('{CSV_FILE}', auto_detect = true)
    """
)

# =============================================================================
# 1. Simple scalar UDF — Celsius to Fahrenheit
# =============================================================================
# conn.create_function(name, callable, param_types, return_type)
# The function receives one Python value per row and returns one value.
print("=== 1. Scalar UDF: Celsius to Fahrenheit ===")


def celsius_to_fahrenheit(c: float) -> float:
    """Convert a temperature from Celsius to Fahrenheit."""
    return c * 9.0 / 5.0 + 32.0


# Register: name used in SQL, Python callable, list of param types, return type.
# Types come from duckdb.sqltypes (e.g. sqltypes.DOUBLE, sqltypes.VARCHAR).
conn.create_function(
    "celsius_to_f",
    celsius_to_fahrenheit,
    [sqltypes.DOUBLE],
    sqltypes.DOUBLE,
)

# Now use it just like a built-in SQL function
conn.sql(
    """
    SELECT
        temp_c,
        ROUND(celsius_to_f(temp_c), 1) AS temp_f
    FROM (VALUES (0), (20), (37), (100)) AS t(temp_c)
    """
).show()

# =============================================================================
# 2. String manipulation UDF — slugify a product name
# =============================================================================
# A "slug" is a URL-safe, lowercase, hyphen-separated string.
# DuckDB doesn't have a built-in slugify, but Python makes it easy.
print("=== 2. String UDF: slugify product names ===")


def slugify(text: str) -> str:
    """Convert a string to a URL-friendly slug."""
    text = text.lower().strip()
    text = re.sub(r"[^a-z0-9]+", "-", text)  # replace non-alphanumeric with hyphens
    return text.strip("-")  # remove leading/trailing hyphens


conn.create_function("slugify", slugify, [sqltypes.VARCHAR], sqltypes.VARCHAR)

conn.sql(
    """
    SELECT
        name,
        slugify(name) AS slug
    FROM (VALUES
        ('Widget Pro'),
        ('Gadget 3000!'),
        ('  Gizmo X-Treme  '),
        ('My Cool Product #1')
    ) AS t(name)
    """
).show()

# =============================================================================
# 3. Type mapping — Python types <-> DuckDB types
# =============================================================================
# When you register a UDF you specify DuckDB types from duckdb.sqltypes.
# DuckDB marshals values between Python and SQL automatically:
#
#   Python type  |  DuckDB type(s)
#   -------------|------------------
#   str          |  VARCHAR
#   int          |  INTEGER, BIGINT, TINYINT, SMALLINT, HUGEINT
#   float        |  DOUBLE, FLOAT
#   bool         |  BOOLEAN
#   None         |  NULL (any type)
#
# Let's prove it with one UDF per type:
print("=== 3. Type mapping: Python <-> DuckDB ===")


def py_upper(s: str) -> str:
    return s.upper()


def py_square(n: int) -> int:
    return n * n


def py_half(x: float) -> float:
    return x / 2.0


def py_negate(b: bool) -> bool:
    return not b


conn.create_function("py_upper", py_upper, [sqltypes.VARCHAR], sqltypes.VARCHAR)
conn.create_function("py_square", py_square, [sqltypes.INTEGER], sqltypes.INTEGER)
conn.create_function("py_half", py_half, [sqltypes.DOUBLE], sqltypes.DOUBLE)
conn.create_function("py_negate", py_negate, [sqltypes.BOOLEAN], sqltypes.BOOLEAN)

conn.sql(
    """
    SELECT
        py_upper('hello')    AS upper_str,    -- str  -> VARCHAR
        py_square(7)         AS squared_int,  -- int  -> INTEGER
        py_half(9.0)         AS halved_dbl,   -- float -> DOUBLE
        py_negate(true)      AS negated_bool  -- bool -> BOOLEAN
    """
).show()

# =============================================================================
# 4. UDF on real data — custom pricing tier
# =============================================================================
# Apply a UDF to the sales table.  This function assigns a pricing tier
# based on the unit price, which could drive downstream business logic.
print("=== 4. UDF on sales.csv: pricing tier ===")


def pricing_tier(price: float) -> str:
    """Classify a unit price into a tier label."""
    if price >= 40.0:
        return "premium"
    elif price >= 20.0:
        return "standard"
    else:
        return "budget"


conn.create_function("pricing_tier", pricing_tier, [sqltypes.DOUBLE], sqltypes.VARCHAR)

conn.sql(
    """
    SELECT
        product,
        price,
        pricing_tier(price) AS tier,
        SUM(quantity)       AS total_qty,
        SUM(quantity * price) AS revenue
    FROM sales
    GROUP BY product, price
    ORDER BY revenue DESC
    """
).show()

# =============================================================================
# 5. NULL handling in UDFs
# =============================================================================
# By default, DuckDB short-circuits: if ANY argument is NULL, the UDF is
# never called and the result is automatically NULL.  This mirrors standard
# SQL NULL propagation and is usually what you want.
#
# If your UDF needs to *see* NULLs (e.g., to replace them with a default),
# pass null_handling=func.FunctionNullHandling.SPECIAL so DuckDB calls the
# function with None.
print("=== 5a. Default NULL handling (propagate NULL) ===")


def label_value(x: int) -> str:
    # This body is never reached when x is NULL.
    return f"value={x}"


conn.create_function("label_value", label_value, [sqltypes.INTEGER], sqltypes.VARCHAR)

conn.sql(
    """
    SELECT
        val,
        label_value(val) AS labeled
    FROM (VALUES (1), (NULL), (3)) AS t(val)
    """
).show()
# Row with NULL val -> labeled is also NULL (function was not called).

print("=== 5b. Special NULL handling (function receives None) ===")


def safe_label(x: int | None) -> str:
    """Handle None explicitly inside the UDF."""
    if x is None:
        return "MISSING"
    return f"value={x}"


# FunctionNullHandling.SPECIAL tells DuckDB to pass None into the function
# instead of short-circuiting to NULL.
conn.create_function(
    "safe_label",
    safe_label,
    [sqltypes.INTEGER],
    sqltypes.VARCHAR,
    null_handling=func.FunctionNullHandling.SPECIAL,
)

conn.sql(
    """
    SELECT
        val,
        safe_label(val) AS labeled
    FROM (VALUES (1), (NULL), (3)) AS t(val)
    """
).show()
# Now the NULL row returns 'MISSING' instead of NULL.

# =============================================================================
# 6. Error handling in UDFs
# =============================================================================
# When a UDF raises a Python exception, DuckDB's default behavior is to
# propagate it and abort the query.  You can change this with the
# exception_handling parameter.
print("=== 6a. Default: exception aborts the query ===")


def strict_discount(price: float) -> float:
    """Apply a 10% discount, but reject non-positive prices."""
    if price <= 0:
        raise ValueError(f"Price must be positive, got {price}")
    return round(price * 0.9, 2)


conn.create_function("strict_discount", strict_discount, [sqltypes.DOUBLE], sqltypes.DOUBLE)

try:
    conn.sql("SELECT strict_discount(-5.0)").fetchall()
except duckdb.InvalidInputException as exc:
    # DuckDB wraps the Python error in an InvalidInputException
    print(f"  Caught: {exc!s:.100s}...")

print()
print("=== 6b. exception_handling=RETURN_NULL — errors become NULL ===")

# Instead of crashing, return NULL for rows that cause an error.
# Useful when processing messy data where a few bad rows are expected.
conn.create_function(
    "lenient_discount",
    strict_discount,
    [sqltypes.DOUBLE],
    sqltypes.DOUBLE,
    exception_handling=duckdb.PythonExceptionHandling.RETURN_NULL,
)

conn.sql(
    """
    SELECT
        price,
        lenient_discount(price) AS discounted
    FROM (VALUES (9.99), (0), (-5.0), (24.99)) AS t(price)
    """
).show()
# Rows with invalid prices get NULL instead of crashing the whole query.

# =============================================================================
# 7. Key patterns and limitations
# =============================================================================
print("=== 7. Key patterns & limitations ===")
print(
    """
Key patterns:
  - conn.create_function(name, fn, [sqltypes.X], sqltypes.Y)
  - Types: sqltypes.VARCHAR, INTEGER, DOUBLE, BOOLEAN, BIGINT, ...
  - null_handling=func.FunctionNullHandling.SPECIAL  -> function receives None
  - exception_handling=duckdb.PythonExceptionHandling.RETURN_NULL  -> errors become NULL
  - UDFs are scoped to the connection that created them

Limitations:
  - Scalar UDFs only (one row in, one value out) -- no table or aggregate UDFs
  - Performance: each row crosses the Python/C boundary, so UDFs are slower
    than native DuckDB functions.  Prefer built-in SQL for hot paths.
  - UDFs cannot issue their own SQL queries inside the function body
  - Type support is limited to simple scalars (no MAP, STRUCT, or LIST params
    unless you use the 'arrow' UDF type for vectorized processing)
  - UDFs are not persisted -- they must be re-registered after reconnecting
"""
)

conn.close()
