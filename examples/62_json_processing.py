"""JSON processing — extract, transform, and build JSON with DuckDB SQL.

DuckDB has a rich set of JSON functions that let you query semi-structured
data without leaving SQL.  This example covers the full lifecycle: reading
raw JSON, extracting fields by path, inspecting structure, bulk-casting
with json_transform, working with JSON arrays, and creating JSON from
relational data.

Demonstrates how to:
  1. Read JSONL and inspect raw JSON structure with json_structure()
  2. Extract values with json_extract / json_extract_string (-> / ->>)
  3. List keys with json_keys
  4. Bulk-cast JSON fields with json_transform
  5. Work with JSON arrays — extracting, filtering, and unnesting
  6. Transform raw JSON into a clean typed table
  7. Create JSON from SQL data — to_json(), json_object(), json_array()

Run with:
    uv run python examples/62_json_processing.py
"""

from __future__ import annotations

from pathlib import Path

import duckdb

# -- Paths --------------------------------------------------------------------
DATA_DIR = Path(__file__).resolve().parent.parent / "src" / "duckdb_demo" / "data"
JSONL_FILE = DATA_DIR / "products.jsonl"

conn = duckdb.connect()

# =============================================================================
# 1. Read products.jsonl and inspect the raw JSON structure
# =============================================================================
# DuckDB's read_json() with auto_detect converts objects to STRUCTs and
# arrays to LISTs automatically.  That's great for quick queries, but
# sometimes you want to keep the raw JSON and work with JSON functions.
#
# Strategy: read each line as plain text with read_csv (using a null
# separator so the whole line stays intact), then cast to the JSON type.
# This gives us a column of type JSON that all json_* functions accept.
print("=== 1. Read JSONL and Inspect JSON Structure ===")

# First, the easy path — auto-detection (for comparison)
print("-- Auto-detected schema (STRUCTs and LISTs) --")
conn.sql(
    f"""
    SELECT * FROM read_json('{JSONL_FILE}', auto_detect = true) LIMIT 3
    """
).show()

# Now read as raw JSON — one JSON-typed column per line.
# chr(0) is a null byte that never appears in JSON text, so each line
# stays as a single value.  We then cast VARCHAR -> JSON.
conn.execute(
    f"""
    CREATE OR REPLACE TABLE products_raw AS
    SELECT column0::JSON AS doc
    FROM read_csv(
        '{JSONL_FILE}',
        header    = false,
        sep       = chr(0),
        columns   = {{'column0': 'VARCHAR'}}
    )
    """
)

print("-- Raw JSON documents (first 3) --")
conn.sql("SELECT doc FROM products_raw LIMIT 3").show()

# json_structure() reveals the shape of a JSON value — the types of every
# nested field, without showing the actual data.  Invaluable for exploring
# unfamiliar JSON payloads.
print("-- json_structure(): type skeleton of the document --")
conn.sql(
    """
    SELECT json_structure(doc) AS structure
    FROM products_raw
    LIMIT 1
    """
).show()

# =============================================================================
# 2. json_extract / json_extract_string — pull values by path
# =============================================================================
# json_extract(json, path)        -> returns a JSON value (typed JSON)
# json_extract_string(json, path) -> returns a VARCHAR  (the text content)
#
# Shorthand operators (just like PostgreSQL):
#   json -> path   ≡ json_extract(json, path)
#   json ->> path  ≡ json_extract_string(json, path)
#
# Paths use dollar-sign notation: '$.field' for objects, '$[0]' for arrays.
print("=== 2. json_extract / json_extract_string (-> / ->>) ===")

# Explicit function names — clearer for beginners
print("-- Explicit function calls --")
conn.sql(
    """
    SELECT
        json_extract(doc, '$.name')           AS name_json,
        json_extract_string(doc, '$.name')    AS name_str,
        json_extract(doc, '$.price')          AS price_json,
        json_extract_string(doc, '$.price')   AS price_str
    FROM products_raw
    LIMIT 4
    """
).show()

# The -> and ->> operators are more concise and idiomatic.
# -> keeps the JSON type, ->> gives you a plain VARCHAR string.
print("-- Shorthand: -> (JSON) vs ->> (VARCHAR) --")
conn.sql(
    """
    SELECT
        doc -> '$.name'            AS name_json,
        doc ->> '$.name'           AS name_str,
        doc -> '$.specs.weight_g'  AS weight_json,
        doc ->> '$.specs.color'    AS color_str
    FROM products_raw
    LIMIT 4
    """
).show()

# Nested path: reach into the "specs" object and sort by a nested field
print("-- Nested extraction: specs.weight_g and specs.color --")
conn.sql(
    """
    SELECT
        doc ->> '$.name'           AS product,
        doc ->> '$.specs.weight_g' AS weight_g,
        doc ->> '$.specs.color'    AS color
    FROM products_raw
    ORDER BY (doc ->> '$.specs.weight_g')::INTEGER DESC
    LIMIT 5
    """
).show()

# =============================================================================
# 3. json_keys — list all keys in a JSON object
# =============================================================================
# json_keys() returns a LIST of the top-level key names.  Great for exploring
# unfamiliar JSON or verifying that documents share the same schema.
print("=== 3. json_keys — List Object Keys ===")

# Top-level keys of the first document
conn.sql(
    """
    SELECT json_keys(doc) AS top_level_keys
    FROM products_raw
    LIMIT 1
    """
).show()

# Keys of the nested "specs" object — pass a sub-expression
print("-- Keys inside the nested 'specs' object --")
conn.sql(
    """
    SELECT json_keys(doc -> '$.specs') AS spec_keys
    FROM products_raw
    LIMIT 1
    """
).show()

# Schema consistency check — do all rows share the same set of keys?
print("-- Do all products have the same top-level keys? --")
conn.sql(
    """
    SELECT
        json_keys(doc) AS keys,
        COUNT(*)       AS num_products
    FROM products_raw
    GROUP BY keys
    """
).show()

# =============================================================================
# 4. json_transform — cast JSON fields to typed columns in bulk
# =============================================================================
# json_transform() is the power tool for JSON processing.  You provide a
# JSON "shape template" that maps each field name to a DuckDB type.  DuckDB
# extracts and casts all fields in one shot, returning a STRUCT.
#
# This is far more efficient (and readable) than calling json_extract on
# every field individually.
print("=== 4. json_transform — Bulk Type Casting ===")

# Transform several fields at once, including the nested specs object
conn.sql(
    """
    SELECT json_transform(
        doc,
        '{
            "id":       "INTEGER",
            "name":     "VARCHAR",
            "category": "VARCHAR",
            "price":    "DOUBLE",
            "specs": {
                "weight_g": "INTEGER",
                "color":    "VARCHAR"
            }
        }'
    ) AS typed
    FROM products_raw
    LIMIT 5
    """
).show()

# Unpack the returned STRUCT into individual columns with .*
# This is the bridge from "one JSON blob" to "clean relational columns".
print("-- Unpack struct fields into columns with .* --")
conn.sql(
    """
    SELECT typed.*
    FROM (
        SELECT json_transform(
            doc,
            '{
                "id":       "INTEGER",
                "name":     "VARCHAR",
                "category": "VARCHAR",
                "price":    "DOUBLE"
            }'
        ) AS typed
        FROM products_raw
    )
    LIMIT 5
    """
).show()

# =============================================================================
# 5. Working with JSON arrays
# =============================================================================
# Products have a "tags" array like ["basic", "popular"].  DuckDB's JSON
# functions let you extract individual elements, check membership, and
# unnest arrays into rows — all from raw JSON.
print("=== 5. Working with JSON Arrays ===")

# Extract array elements by index (zero-based)
print("-- Extract individual array elements by index --")
conn.sql(
    """
    SELECT
        doc ->> '$.name'     AS product,
        doc -> '$.tags'      AS tags_json,
        doc ->> '$.tags[0]'  AS first_tag,
        doc ->> '$.tags[1]'  AS second_tag
    FROM products_raw
    LIMIT 5
    """
).show()

# json_array_length() counts elements in a JSON array
print("-- json_array_length(): how many tags per product? --")
conn.sql(
    """
    SELECT
        doc ->> '$.name'                  AS product,
        json_array_length(doc, '$.tags')  AS num_tags,
        doc -> '$.tags'                   AS tags
    FROM products_raw
    ORDER BY num_tags DESC
    """
).show()

# Filter: find products where a specific value exists in the tags array.
# json_contains() checks if a JSON array/object contains a given value.
# Note: the search value must be valid JSON — wrap strings in double quotes.
print("-- Filter: products with 'premium' tag (json_contains) --")
conn.sql(
    """
    SELECT
        doc ->> '$.name'  AS product,
        doc ->> '$.price' AS price,
        doc -> '$.tags'   AS tags
    FROM products_raw
    WHERE json_contains(doc -> '$.tags', '"premium"')
    ORDER BY (doc ->> '$.price')::DOUBLE DESC
    """
).show()

# Unnest: expand tags into individual rows for aggregation.
# First use json_transform to convert JSON array -> DuckDB LIST, then unnest.
print("-- Unnest tags: one row per product-tag pair --")
conn.sql(
    """
    SELECT
        doc ->> '$.name' AS product,
        unnest(
            json_transform(doc, '{"tags": ["VARCHAR"]}').tags
        ) AS tag
    FROM products_raw
    ORDER BY tag, product
    """
).show()

# Count products per tag — a common analytics pattern with JSON arrays
print("-- Aggregate: product count per tag --")
conn.sql(
    """
    WITH tags_expanded AS (
        SELECT
            doc ->> '$.name' AS product,
            unnest(
                json_transform(doc, '{"tags": ["VARCHAR"]}').tags
            ) AS tag
        FROM products_raw
    )
    SELECT
        tag,
        COUNT(*)                                    AS num_products,
        STRING_AGG(product, ', ' ORDER BY product)  AS products
    FROM tags_expanded
    GROUP BY tag
    ORDER BY num_products DESC
    """
).show()

# =============================================================================
# 6. JSON to structured table — full transformation
# =============================================================================
# Real-world pattern: read raw JSON, transform it into a fully typed table
# with clean column names, flattened nested objects, and no JSON types left.
# This is the "landing zone → clean table" pattern common in ETL.
print("=== 6. JSON to Structured Table — Full Transformation ===")

conn.execute(
    """
    CREATE OR REPLACE TABLE products AS
    SELECT
        -- Cast scalar fields from JSON to native DuckDB types
        (doc ->> '$.id')::INTEGER             AS product_id,
        doc ->> '$.name'                      AS product_name,
        doc ->> '$.category'                  AS category,
        (doc ->> '$.price')::DOUBLE           AS price,
        -- Flatten the nested specs object into top-level columns
        (doc ->> '$.specs.weight_g')::INTEGER AS weight_grams,
        doc ->> '$.specs.color'               AS color,
        -- Convert JSON array to native DuckDB LIST for easy querying
        json_transform(doc, '{"tags": ["VARCHAR"]}').tags AS tags
    FROM products_raw
    """
)

# Verify the schema — all clean DuckDB types, no JSON anywhere
print("-- Schema of the transformed table --")
conn.sql("DESCRIBE products").show()

# Query the clean table — no more JSON path extraction needed!
print("-- Query the clean typed table --")
conn.sql(
    """
    SELECT product_id, product_name, category, price, weight_grams, color, tags
    FROM products
    WHERE price > 20
    ORDER BY price DESC
    """
).show()

# Native LIST functions work directly on the tags column
print("-- Filter on tags using list_contains (native LIST, not JSON) --")
conn.sql(
    """
    SELECT product_name, price, tags
    FROM products
    WHERE list_contains(tags, 'new')
    ORDER BY price
    """
).show()

# =============================================================================
# 7. Creating JSON from SQL data — to_json(), json_object(), json_array()
# =============================================================================
# Sometimes you need to go the other direction: take relational data and
# produce JSON output for an API, a config file, or a downstream system.
print("=== 7. Creating JSON from SQL Data ===")

# to_json() converts a struct (or row literal) into a JSON string
print("-- to_json(): convert a struct to JSON --")
conn.sql(
    """
    SELECT to_json(
        {id: product_id, name: product_name, price: price}
    ) AS product_json
    FROM products
    LIMIT 3
    """
).show()

# json_object() builds a JSON object from alternating key-value pairs.
# Keys must be strings; values can be any type — DuckDB serializes them.
print("-- json_object(): build JSON from key-value pairs --")
conn.sql(
    """
    SELECT json_object(
        'product',   product_name,
        'category',  category,
        'price_usd', price,
        'heavy',     weight_grams > 200
    ) AS custom_json
    FROM products
    LIMIT 4
    """
).show()

# json_array() creates a JSON array from a list of values
print("-- json_array(): build a JSON array --")
conn.sql(
    """
    SELECT json_array(product_name, category, price) AS product_tuple
    FROM products
    LIMIT 4
    """
).show()

# Combine json_object + json_group_array to build nested JSON output.
# Example: a category summary with an embedded list of products.
print("-- Nested JSON: category summary with embedded product list --")
conn.sql(
    """
    SELECT json_object(
        'category',     category,
        'num_products', COUNT(*),
        'avg_price',    ROUND(AVG(price), 2),
        'products',     json_group_array(
            json_object('name', product_name, 'price', price)
        )
    ) AS category_json
    FROM products
    GROUP BY category
    ORDER BY category
    """
).show()

# =============================================================================
# Summary
# =============================================================================
print("=== Summary ===")
print(
    """
Key JSON functions in DuckDB:

  Reading & inspecting:
    json_structure(json)           -- type skeleton of a JSON value
    json_keys(json)                -- list top-level keys
    json_array_length(json, path)  -- count elements in an array

  Extracting:
    json_extract(json, path)        -- extract as JSON  (shorthand: ->)
    json_extract_string(json, path) -- extract as VARCHAR (shorthand: ->>)
    json_transform(json, shape)     -- bulk extract + cast to a STRUCT

  Filtering:
    json_contains(json, value)     -- check if JSON contains a value

  Creating:
    to_json(struct)                -- struct/row to JSON string
    json_object(k1, v1, ...)       -- build a JSON object
    json_array(v1, v2, ...)        -- build a JSON array
    json_group_array(expr)         -- aggregate rows into a JSON array

  Path syntax:
    '$.field'         -- object field
    '$.nested.field'  -- nested field
    '$.array[0]'      -- array element (zero-based)

Tip: Use read_json(path, auto_detect=true) when you just want typed
columns.  To work with raw JSON functions, read lines as text and cast
to JSON — this gives you full control over extraction and transformation.
"""
)

conn.close()
