"""Recursive CTEs — traverse trees, walk graphs, and flatten hierarchies with SQL.

WITH RECURSIVE lets a CTE reference itself, producing rows iteratively until
a termination condition is met.  This unlocks tree traversal, bill-of-materials
explosion, graph reachability, and more — all in pure SQL.

Run with:
    uv run python examples/63_recursive_ctes.py
"""

from __future__ import annotations

import duckdb

conn = duckdb.connect()

# =============================================================================
# 1. Simple number sequence — the "hello world" of recursive CTEs
# =============================================================================
# A recursive CTE has two parts joined by UNION ALL:
#   - Base case: the non-recursive SELECT that seeds the first row(s)
#   - Recursive step: a SELECT that references the CTE itself
# DuckDB repeats the recursive step until it produces zero new rows.
print("=== 1. Simple number sequence (1..10) ===")

conn.sql(
    """
    WITH RECURSIVE nums AS (
        -- Base case: start at 1
        SELECT 1 AS n

        UNION ALL

        -- Recursive step: add 1 until we reach 10
        SELECT n + 1
        FROM nums
        WHERE n < 10
    )
    SELECT n FROM nums
    """
).show()

# How it works, step by step:
#   Iteration 0 (base):  {1}
#   Iteration 1:         {2}    (1 + 1, and 1 < 10 is true)
#   Iteration 2:         {3}
#   ...
#   Iteration 9:         {10}   (9 + 1, and 9 < 10 is true)
#   Iteration 10:        {}     (10 < 10 is false -> stop)

# =============================================================================
# 2. Org chart hierarchy — find all reports (direct + indirect)
# =============================================================================
# Classic use case: an employee table where each row points to its manager.
# We want every person who reports to a given manager, at any depth.
print("=== 2. Org chart: all reports under 'Alice' ===")

conn.execute(
    """
    CREATE OR REPLACE TABLE employees (
        emp_id   INTEGER PRIMARY KEY,
        name     VARCHAR NOT NULL,
        manager_id INTEGER  -- NULL for the CEO (no manager)
    )
    """
)
conn.execute(
    """
    INSERT INTO employees VALUES
        (1, 'Alice',   NULL),  -- CEO
        (2, 'Bob',     1),     -- reports to Alice
        (3, 'Carol',   1),     -- reports to Alice
        (4, 'Dave',    2),     -- reports to Bob
        (5, 'Eve',     2),     -- reports to Bob
        (6, 'Frank',   3),     -- reports to Carol
        (7, 'Grace',   4),     -- reports to Dave
        (8, 'Heidi',   6)      -- reports to Frank
    """
)
# Org chart looks like:
#
#            Alice (1)
#           /         \
#       Bob (2)     Carol (3)
#       /    \          \
#   Dave(4) Eve(5)    Frank(6)
#     |                 |
#   Grace(7)          Heidi(8)

conn.sql(
    """
    WITH RECURSIVE reports AS (
        -- Base case: the manager we're interested in
        SELECT emp_id, name, manager_id, 'self' AS relationship
        FROM employees
        WHERE name = 'Alice'

        UNION ALL

        -- Recursive step: find employees whose manager is already in our set
        SELECT e.emp_id, e.name, e.manager_id, 'report'
        FROM employees e
        JOIN reports r ON e.manager_id = r.emp_id
    )
    -- Exclude Alice herself to see only her reports
    SELECT emp_id, name, manager_id, relationship
    FROM reports
    WHERE relationship != 'self'
    ORDER BY emp_id
    """
).show()

# =============================================================================
# 3. Tree traversal with path — build the full org path string
# =============================================================================
# Same org chart, but now we build a breadcrumb trail showing the chain of
# command: CEO > VP > Director > ... for every employee.
print("=== 3. Tree traversal: full path from CEO to each employee ===")

conn.sql(
    """
    WITH RECURSIVE org_path AS (
        -- Base case: the root node (CEO has no manager)
        SELECT
            emp_id,
            name,
            name AS path,       -- path starts with just the CEO's name
            0    AS depth
        FROM employees
        WHERE manager_id IS NULL

        UNION ALL

        -- Recursive step: append child's name to parent's path
        SELECT
            e.emp_id,
            e.name,
            op.path || ' > ' || e.name AS path,
            op.depth + 1               AS depth
        FROM employees e
        JOIN org_path op ON e.manager_id = op.emp_id
    )
    SELECT emp_id, name, depth, path
    FROM org_path
    ORDER BY path
    """
).show()

# =============================================================================
# 4. Bill of materials — recursive cost roll-up
# =============================================================================
# A product is made of parts; parts can contain sub-parts.  We need the total
# material cost of the top-level product by "exploding" the BOM recursively.
print("=== 4. Bill of materials: total cost of a 'Bicycle' ===")

conn.execute(
    """
    CREATE OR REPLACE TABLE bom (
        parent_part  VARCHAR,
        child_part   VARCHAR,
        quantity     INTEGER   -- how many child parts per parent
    )
    """
)
conn.execute(
    """
    INSERT INTO bom VALUES
        ('Bicycle',    'Frame',       1),
        ('Bicycle',    'Wheel',       2),
        ('Bicycle',    'Handlebar',   1),
        ('Frame',      'Tube',        3),
        ('Frame',      'Joint',       4),
        ('Wheel',      'Rim',         1),
        ('Wheel',      'Spoke',      32),
        ('Wheel',      'Tire',        1),
        ('Handlebar',  'Grip',        2),
        ('Handlebar',  'Stem',        1)
    """
)

# Leaf-part costs (parts that aren't composed of sub-parts)
conn.execute(
    """
    CREATE OR REPLACE TABLE part_costs (
        part   VARCHAR PRIMARY KEY,
        cost   DECIMAL(10, 2)
    )
    """
)
conn.execute(
    """
    INSERT INTO part_costs VALUES
        ('Tube',   5.00),
        ('Joint',  1.50),
        ('Rim',   12.00),
        ('Spoke',  0.25),
        ('Tire',   8.00),
        ('Grip',   2.00),
        ('Stem',   6.00)
    """
)
# BOM tree:
#   Bicycle
#   |- Frame  x1  -> Tube x3 ($5), Joint x4 ($1.50)
#   |- Wheel  x2  -> Rim x1 ($12), Spoke x32 ($0.25), Tire x1 ($8)
#   |- Handlebar x1 -> Grip x2 ($2), Stem x1 ($6)

conn.sql(
    """
    WITH RECURSIVE exploded AS (
        -- Base case: direct children of the Bicycle
        SELECT
            b.child_part  AS part,
            b.quantity     AS total_qty,
            1              AS depth
        FROM bom b
        WHERE b.parent_part = 'Bicycle'

        UNION ALL

        -- Recursive step: expand sub-parts, multiplying quantities
        SELECT
            b.child_part,
            e.total_qty * b.quantity,   -- cumulative quantity
            e.depth + 1
        FROM bom b
        JOIN exploded e ON b.parent_part = e.part
    ),
    -- Now join leaf parts to their costs
    costed AS (
        SELECT
            e.part,
            e.total_qty,
            e.depth,
            COALESCE(pc.cost, 0) AS unit_cost,
            e.total_qty * COALESCE(pc.cost, 0) AS line_cost
        FROM exploded e
        LEFT JOIN part_costs pc ON e.part = pc.part
    )
    SELECT
        part,
        total_qty,
        unit_cost,
        line_cost
    FROM costed
    WHERE unit_cost > 0   -- leaf parts only (they have a cost)
    ORDER BY line_cost DESC
    """
).show()

# Show grand total
conn.sql(
    """
    WITH RECURSIVE exploded AS (
        SELECT b.child_part AS part, b.quantity AS total_qty
        FROM bom b
        WHERE b.parent_part = 'Bicycle'

        UNION ALL

        SELECT b.child_part, e.total_qty * b.quantity
        FROM bom b
        JOIN exploded e ON b.parent_part = e.part
    )
    SELECT
        'Bicycle' AS product,
        SUM(e.total_qty * pc.cost) AS total_material_cost
    FROM exploded e
    JOIN part_costs pc ON e.part = pc.part
    """
).show()

# =============================================================================
# 5. Graph walk — find all reachable nodes from a starting node
# =============================================================================
# Unlike a tree, a graph can have multiple paths between nodes.
# We use UNION (not UNION ALL) to deduplicate and prevent infinite looping
# — more on that in Section 7.
print("=== 5. Graph walk: all cities reachable from 'A' ===")

conn.execute(
    """
    CREATE OR REPLACE TABLE edges (src VARCHAR, dst VARCHAR)
    """
)
conn.execute(
    """
    INSERT INTO edges VALUES
        ('A', 'B'),
        ('A', 'C'),
        ('B', 'D'),
        ('C', 'D'),
        ('D', 'E'),
        ('B', 'E'),
        ('E', 'F')
    """
)
# Graph:
#   A -> B -> D -> E -> F
#   |    |    ^
#   +--> C ---+
#   B ------> E

conn.sql(
    """
    WITH RECURSIVE reachable AS (
        -- Base case: starting node
        SELECT 'A' AS node, 0 AS hops

        UNION

        -- Recursive step: follow edges from already-reached nodes
        -- UNION (not UNION ALL) deduplicates so each node appears once
        SELECT e.dst, r.hops + 1
        FROM edges e
        JOIN reachable r ON e.src = r.node
    )
    SELECT node, MIN(hops) AS min_hops   -- shortest distance from A
    FROM reachable
    GROUP BY node
    ORDER BY min_hops, node
    """
).show()

# =============================================================================
# 6. Hierarchy flattening — tree to flat table with depth/level
# =============================================================================
# A common ETL pattern: take a self-referencing hierarchy and flatten it into
# a table with explicit depth and root columns, ready for BI dashboards.
print("=== 6. Hierarchy flattening: employees with level and root ===")

conn.sql(
    """
    WITH RECURSIVE flat AS (
        -- Base case: root nodes (employees with no manager)
        SELECT
            emp_id,
            name,
            manager_id,
            name     AS root,       -- the root of this subtree
            0        AS level,
            name     AS breadcrumb  -- the path from root to here
        FROM employees
        WHERE manager_id IS NULL

        UNION ALL

        -- Recursive step: attach children one level deeper
        SELECT
            e.emp_id,
            e.name,
            e.manager_id,
            f.root,
            f.level + 1,
            f.breadcrumb || ' / ' || e.name
        FROM employees e
        JOIN flat f ON e.manager_id = f.emp_id
    )
    SELECT
        level,
        REPEAT('  ', level) || name AS indented_name,  -- visual indent
        root,
        breadcrumb
    FROM flat
    ORDER BY breadcrumb
    """
).show()

# =============================================================================
# 7. Practical safeguard: UNION vs UNION ALL in cyclic graphs
# =============================================================================
# If your data has cycles (A -> B -> A), UNION ALL will loop forever because
# it keeps producing the same rows.  UNION deduplicates each iteration, so a
# node that's already been visited won't generate new rows — the recursion
# stops naturally.
print("=== 7. Safeguard: UNION prevents infinite loops in cyclic graphs ===")

conn.execute(
    """
    CREATE OR REPLACE TABLE cyclic_edges (src VARCHAR, dst VARCHAR)
    """
)
conn.execute(
    """
    INSERT INTO cyclic_edges VALUES
        ('X', 'Y'),
        ('Y', 'Z'),
        ('Z', 'X'),   -- cycle! Z points back to X
        ('Z', 'W')
    """
)
# Graph:  X -> Y -> Z -> W
#          ^        |
#          +--------+   (cycle)

# With UNION (safe) — each node appears at most once, no infinite loop
conn.sql(
    """
    WITH RECURSIVE walk AS (
        SELECT 'X' AS node

        UNION            -- UNION deduplicates, so 'X' won't be re-added

        SELECT ce.dst
        FROM cyclic_edges ce
        JOIN walk w ON ce.src = w.node
    )
    SELECT node FROM walk ORDER BY node
    """
).show()

# For comparison, here's what UNION ALL would attempt (DO NOT run on large
# cyclic graphs — it will run until DuckDB hits its iteration limit):
print("  (UNION ALL on a cycle would revisit X->Y->Z->X->Y->... endlessly)")
print("  DuckDB imposes a default recursion limit to prevent true infinite loops,")
print("  but UNION is the correct, intentional fix for cyclic data.")
print()

# =============================================================================
# Summary
# =============================================================================
print("=== Summary: WITH RECURSIVE patterns ===")
print(
    """
Pattern             | Technique
--------------------|------------------------------------------------------
Number sequence     | Increment a counter with a WHERE stop condition
Org chart / tree    | JOIN children to parents already in the CTE
Path building       | Concatenate names with || as you recurse
Bill of materials   | Multiply quantities at each level, then sum leaf costs
Graph reachability  | Follow edges; use UNION to deduplicate visited nodes
Hierarchy flatten   | Track depth + root + breadcrumb in each recursive row
Cycle safety        | UNION (not UNION ALL) prevents infinite revisitation

Termination rules:
  - The recursion stops when the recursive step produces zero new rows.
  - UNION automatically drops rows that already exist in the result set.
  - For UNION ALL, you MUST ensure the WHERE clause eventually excludes
    all rows (e.g., depth < N, or no matching join partners).
"""
)

conn.close()
