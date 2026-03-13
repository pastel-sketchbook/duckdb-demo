"""Nested types — STRUCT, LIST, and MAP in DuckDB.

DuckDB has first-class support for complex/nested types that let you model
rich, denormalized data directly in SQL.  This example covers creating and
querying STRUCTs, LISTs, and MAPs, plus the key functions for working with
them: unnest(), list_aggregate(), list_filter(), list_transform(), and more.

Run with:
    uv run python examples/61_nested_types.py
"""

from __future__ import annotations

import duckdb

conn = duckdb.connect()

# =============================================================================
# 1. Creating STRUCTs — inline struct literals, named fields, dot notation
# =============================================================================
# A STRUCT is a fixed set of named fields (like a Python dict with known keys).
# Create them with {'field': value} syntax or the struct_pack() function.
print("=== 1. Creating STRUCTs ===")

# Inline struct literal — DuckDB infers the type automatically
conn.sql(
    """
    SELECT {'name': 'Alice', 'age': 30, 'active': true} AS person
    """
).show()

# Access individual fields with dot notation
conn.sql(
    """
    SELECT
        person.name   AS name,
        person.age    AS age,
        person.active AS active
    FROM (
        SELECT {'name': 'Alice', 'age': 30, 'active': true} AS person
    )
    """
).show()

# struct_pack() is the function form — useful when field names are dynamic
conn.sql(
    """
    SELECT struct_pack(city := 'Denver', state := 'CO', pop := 711463) AS place
    """
).show()

# STRUCTs from table data using row() or named struct syntax
print("--- STRUCTs built from table rows ---")
conn.sql(
    """
    SELECT
        id,
        {'first': first_name, 'last': last_name} AS full_name,
        {'city': city, 'state': state}            AS address
    FROM (VALUES
        (1, 'Alice', 'Smith',   'Denver',   'CO'),
        (2, 'Bob',   'Jones',   'Seattle',  'WA'),
        (3, 'Carol', 'Lee',     'Austin',   'TX')
    ) AS t(id, first_name, last_name, city, state)
    """
).show()

# =============================================================================
# 2. Creating LISTs — list literals, list_value(), index access
# =============================================================================
# A LIST is an ordered, variable-length sequence of values of the same type.
# Think Python lists or JSON arrays.
print("=== 2. Creating LISTs ===")

# List literal with bracket syntax
conn.sql(
    """
    SELECT [1, 2, 3, 4, 5] AS numbers
    """
).show()

# list_value() is the function form
conn.sql(
    """
    SELECT list_value('red', 'green', 'blue') AS colors
    """
).show()

# Indexing — DuckDB lists are 1-based!
conn.sql(
    """
    SELECT
        colors[1] AS first,
        colors[2] AS second,
        colors[3] AS third
    FROM (SELECT ['red', 'green', 'blue'] AS colors)
    """
).show()

# list_aggr / array_agg — building lists from grouped rows
print("--- Building lists from grouped data ---")
conn.sql(
    """
    SELECT
        department,
        list(employee ORDER BY employee) AS employees,
        list(salary ORDER BY salary DESC) AS salaries
    FROM (VALUES
        ('Engineering', 'Alice',  120000),
        ('Engineering', 'Bob',     95000),
        ('Engineering', 'Carol',  110000),
        ('Sales',       'Dave',    80000),
        ('Sales',       'Eve',     85000)
    ) AS t(department, employee, salary)
    GROUP BY department
    """
).show()

# =============================================================================
# 3. Creating MAPs — map literals, map_keys(), map_values(), element access
# =============================================================================
# A MAP is a key-value collection where keys and values each have a uniform type.
# Unlike STRUCTs, the keys don't have to be known at schema time.
print("=== 3. Creating MAPs ===")

# Map literal — MAP {key: value, ...}
conn.sql(
    """
    SELECT MAP {
        'color': 'blue',
        'size':  'large',
        'fit':   'regular'
    } AS attributes
    """
).show()

# map_keys() and map_values() — extract keys or values as lists
conn.sql(
    """
    SELECT
        map_keys(attrs)   AS keys,
        map_values(attrs) AS vals
    FROM (
        SELECT MAP {'color': 'blue', 'size': 'large', 'fit': 'regular'} AS attrs
    )
    """
).show()

# Element access with bracket notation — returns the value for a key
conn.sql(
    """
    SELECT
        attrs['color'] AS color,
        attrs['size']  AS size
    FROM (
        SELECT MAP {'color': 'blue', 'size': 'large', 'fit': 'regular'} AS attrs
    )
    """
).show()

# map_from_entries() — build a MAP from a list of key-value structs
print("--- map_from_entries(): build maps dynamically ---")
conn.sql(
    """
    SELECT map_from_entries([
        {'key': 'cpu',  'value': '8 cores'},
        {'key': 'ram',  'value': '32 GB'},
        {'key': 'disk', 'value': '1 TB SSD'}
    ]) AS spec
    """
).show()

# =============================================================================
# 4. unnest() — exploding lists into rows
# =============================================================================
# unnest() is the most common nested-type operation.  It takes a LIST column
# and produces one row per element, duplicating the other columns.
print("=== 4. unnest() — exploding lists into rows ===")

conn.sql(
    """
    SELECT
        name,
        unnest(hobbies) AS hobby
    FROM (VALUES
        ('Alice', ['hiking', 'chess', 'painting']),
        ('Bob',   ['gaming', 'cooking']),
        ('Carol', ['running'])
    ) AS t(name, hobbies)
    """
).show()

# unnest with ordinality — get the position of each element
print("--- unnest with generate_subscripts for position ---")
conn.sql(
    """
    WITH data AS (
        SELECT 'Alice' AS name, ['hiking', 'chess', 'painting'] AS hobbies
    )
    SELECT
        name,
        unnest(hobbies) AS hobby,
        generate_subscripts(hobbies, 1) AS position
    FROM data
    """
).show()

# unnest a STRUCT — spreads fields into separate columns
print("--- unnest a STRUCT into columns ---")
conn.sql(
    """
    SELECT
        id,
        unnest(info)   -- expands struct fields into columns
    FROM (VALUES
        (1, {'city': 'Denver',  'state': 'CO'}),
        (2, {'city': 'Seattle', 'state': 'WA'})
    ) AS t(id, info)
    """
).show()

# =============================================================================
# 5. Nested structures — STRUCT containing LIST, LIST of STRUCTs
# =============================================================================
# The real power comes from combining these types.
print("=== 5. Nested structures ===")

# A STRUCT that contains a LIST field
print("--- STRUCT with a LIST field ---")
conn.sql(
    """
    SELECT {
        'name':    'Engineering',
        'members': ['Alice', 'Bob', 'Carol'],
        'budget':  500000
    } AS team
    """
).show()

# Access the nested list, then unnest it
conn.sql(
    """
    SELECT
        team.name        AS team_name,
        team.budget      AS budget,
        unnest(team.members) AS member
    FROM (
        SELECT {
            'name':    'Engineering',
            'members': ['Alice', 'Bob', 'Carol'],
            'budget':  500000
        } AS team
    )
    """
).show()

# A LIST of STRUCTs — very common for denormalized data (like JSON arrays)
print("--- LIST of STRUCTs ---")
conn.sql(
    """
    SELECT [
        {'product': 'Widget', 'qty': 3, 'price': 9.99},
        {'product': 'Gadget', 'qty': 1, 'price': 24.99},
        {'product': 'Gizmo',  'qty': 2, 'price': 49.99}
    ] AS line_items
    """
).show()

# Unnest the list of structs — each struct becomes a row, fields become columns
conn.sql(
    """
    SELECT unnest(line_items)
    FROM (
        SELECT [
            {'product': 'Widget', 'qty': 3, 'price': 9.99},
            {'product': 'Gadget', 'qty': 1, 'price': 24.99},
            {'product': 'Gizmo',  'qty': 2, 'price': 49.99}
        ] AS line_items
    )
    """
).show()

# =============================================================================
# 6. List functions — aggregate, filter, transform, sort, contains
# =============================================================================
print("=== 6. List functions ===")

# list_aggregate (alias: list_aggr) — apply an aggregate function to a list
print("--- list_aggregate: sum, avg, min, max of a list ---")
conn.sql(
    """
    SELECT
        numbers,
        list_aggregate(numbers, 'sum')               AS total,
        list_aggregate(numbers, 'avg')                AS average,
        list_aggregate(numbers, 'min')                AS minimum,
        list_aggregate(numbers, 'max')                AS maximum,
        list_aggregate(numbers, 'string_agg', ', ')   AS joined
    FROM (VALUES
        ([10, 20, 30, 40, 50]),
        ([5, 15, 25]),
        ([100])
    ) AS t(numbers)
    """
).show()

# list_filter — keep only elements matching a lambda condition
print("--- list_filter: keep elements matching a condition ---")
conn.sql(
    """
    SELECT
        numbers,
        list_filter(numbers, x -> x > 20)  AS above_20,
        list_filter(numbers, x -> x % 2 = 0) AS evens
    FROM (VALUES
        ([10, 20, 30, 40, 50]),
        ([5, 15, 25, 35]),
        ([2, 4, 6, 8])
    ) AS t(numbers)
    """
).show()

# list_transform — apply a function to every element (like Python's map())
print("--- list_transform: apply a function to each element ---")
conn.sql(
    """
    SELECT
        names,
        list_transform(names, x -> upper(x))       AS uppercased,
        list_transform(names, x -> length(x))       AS name_lengths,
        list_transform(names, x -> concat('Dr. ', x)) AS with_title
    FROM (VALUES
        (['alice', 'bob', 'carol']),
        (['dave', 'eve'])
    ) AS t(names)
    """
).show()

# list_sort and list_reverse_sort
print("--- list_sort / list_reverse_sort ---")
conn.sql(
    """
    SELECT
        scores,
        list_sort(scores)         AS ascending,
        list_reverse_sort(scores) AS descending
    FROM (VALUES
        ([88, 72, 95, 61, 84]),
        ([100, 50, 75])
    ) AS t(scores)
    """
).show()

# list_contains and list_has_any — membership tests
print("--- list_contains / list_has_any ---")
conn.sql(
    """
    SELECT
        tags,
        list_contains(tags, 'python')              AS has_python,
        list_contains(tags, 'rust')                AS has_rust,
        list_has_any(tags, ['python', 'typescript']) AS has_py_or_ts
    FROM (VALUES
        (['python', 'sql', 'duckdb']),
        (['rust', 'go', 'zig']),
        (['typescript', 'react'])
    ) AS t(tags)
    """
).show()

# list_distinct and flatten — dedup and flatten nested lists
print("--- list_distinct / flatten ---")
conn.sql(
    """
    SELECT
        list_distinct([1, 2, 2, 3, 3, 3])          AS deduped,
        flatten([[1, 2], [3, 4], [5]])              AS flattened
    """
).show()

# =============================================================================
# 7. Practical example: denormalized "order with line items"
# =============================================================================
# Real-world pattern: build a nested order document with embedded line items,
# then query it.  This is the kind of structure you'd see in JSON APIs or
# document databases.
print("=== 7. Practical: order with nested line items ===")

# Step 1: Create normalized tables (orders + line_items)
conn.execute(
    """
    CREATE TABLE orders AS
    SELECT * FROM (VALUES
        (1001, '2024-01-15', 'Alice'),
        (1002, '2024-01-16', 'Bob'),
        (1003, '2024-01-16', 'Carol')
    ) AS t(order_id, order_date, customer)
    """
)

conn.execute(
    """
    CREATE TABLE line_items AS
    SELECT * FROM (VALUES
        (1001, 'Widget',  3, 9.99),
        (1001, 'Gadget',  1, 24.99),
        (1001, 'Gizmo',   2, 49.99),
        (1002, 'Widget', 10, 9.99),
        (1002, 'Gizmo',   1, 49.99),
        (1003, 'Gadget',  5, 24.99)
    ) AS t(order_id, product, quantity, unit_price)
    """
)

# Step 2: Build a denormalized structure — each order row contains a LIST
# of STRUCT line items.  This is a very common pattern for API responses
# and document-oriented data.
print("--- Denormalized order documents ---")
conn.sql(
    """
    SELECT
        o.order_id,
        o.order_date,
        o.customer,
        -- Aggregate line items into a LIST of STRUCTs
        list({
            'product':    li.product,
            'quantity':   li.quantity,
            'unit_price': li.unit_price,
            'line_total': ROUND(li.quantity * li.unit_price, 2)
        } ORDER BY li.product) AS items,
        -- Summary fields computed from the line items
        SUM(li.quantity)                         AS total_qty,
        ROUND(SUM(li.quantity * li.unit_price), 2) AS order_total
    FROM orders o
    JOIN line_items li ON o.order_id = li.order_id
    GROUP BY o.order_id, o.order_date, o.customer
    ORDER BY o.order_id
    """
).show()

# Step 3: Query the nested structure — unnest to get back to rows
# Two approaches: unnest into a struct column, or unnest + dot notation for flat columns
print("--- Unnest the order documents back to flat rows ---")
conn.sql(
    """
    WITH order_docs AS (
        SELECT
            o.order_id,
            o.customer,
            list({
                'product':    li.product,
                'quantity':   li.quantity,
                'unit_price': li.unit_price,
                'line_total': ROUND(li.quantity * li.unit_price, 2)
            }) AS items
        FROM orders o
        JOIN line_items li ON o.order_id = li.order_id
        GROUP BY o.order_id, o.customer
    ),
    -- First unnest: one row per item (item is still a struct)
    exploded AS (
        SELECT order_id, customer, unnest(items) AS item
        FROM order_docs
    )
    -- Then use dot notation to pull out individual fields
    SELECT
        order_id,
        customer,
        item.product    AS product,
        item.quantity   AS quantity,
        item.unit_price AS unit_price,
        item.line_total AS line_total
    FROM exploded
    ORDER BY order_id, product
    """
).show()

# Step 4: Use list functions on the nested items — e.g. find high-value lines
print("--- Filter line items within each order (list_filter) ---")
conn.sql(
    """
    WITH order_docs AS (
        SELECT
            o.order_id,
            o.customer,
            list({
                'product':    li.product,
                'quantity':   li.quantity,
                'unit_price': li.unit_price,
                'line_total': ROUND(li.quantity * li.unit_price, 2)
            }) AS items
        FROM orders o
        JOIN line_items li ON o.order_id = li.order_id
        GROUP BY o.order_id, o.customer
    )
    SELECT
        order_id,
        customer,
        -- Keep only line items where the total exceeds $30
        list_filter(items, item -> item.line_total > 30) AS big_items,
        -- Sum of all line totals using list_transform + list_aggregate
        list_aggregate(
            list_transform(items, item -> item.line_total),
            'sum'
        ) AS order_total
    FROM order_docs
    ORDER BY order_id
    """
).show()

conn.close()
