# Track 2 — Core SQL: Rationale

## Intention

Track 2 builds the **everyday SQL toolkit** that every data analyst uses in
every query. After Track 1 proved "DuckDB can read my files," Track 2 answers
"now what can I actually *do* with the data?" The six examples progress from
simple row filtering to composable query building blocks (CTEs, CASE, set ops).

By the end of Track 2, a learner should be able to write production-quality
analytical queries against any tabular dataset.

## Why these six examples, in this order

### 21 — Filtering & sorting

Every SQL journey starts with `WHERE`. This example is intentionally
comprehensive — it covers `WHERE`, `AND`/`OR`, `BETWEEN`, `IN`, `LIKE`,
`IS NULL`, `ORDER BY`, `LIMIT`, `OFFSET`, and `NOT` in a single progression.

Design choices:

- **Inserted a NULL row mid-script** to demonstrate `IS NULL` vs `= NULL` — a
  perennial beginner trap. Seeing the NULL row appear and then being filtered
  is more memorable than reading about it.
- **LIMIT + OFFSET for pagination** is included because learners will encounter
  it in every web application backend. Showing it early normalizes the pattern.
- **Logical progression**: starts with the simplest filter (equality), adds
  complexity one clause at a time. No section depends on understanding the next.

### 22 — Aggregations

Aggregation is *the* defining operation of analytics. This example covers the
full spectrum:

- **Basic aggregates** (COUNT, SUM, AVG, MIN, MAX) in a single summary query.
- **GROUP BY** with one column, then multiple columns — building intuition for
  how grouping splits data into buckets.
- **HAVING** — contrasted explicitly against WHERE. The "WHERE filters rows,
  HAVING filters groups" distinction is a top-5 SQL interview question.
- **COUNT(DISTINCT)** — important enough to warrant its own section.
- **GROUP BY ALL** — DuckDB's ergonomic extension. Showing it *after* the manual
  GROUP BY teaches the concept before the shortcut.
- **FILTER clause** — a DuckDB/SQL standard feature that most learners haven't
  seen. Introducing it here avoids the need for CASE-inside-SUM (which is taught
  properly in example 25).
- **Percentage of total** — the most common derived metric in business reporting.

### 23 — JOINs

JOINs are the second pillar of SQL (after aggregation). This example uses
`sales` + `products` — two tables the learner already knows from Track 1 — so
the focus stays on JOIN mechanics, not data comprehension.

Coverage:

- **INNER JOIN** — the default and most common.
- **LEFT JOIN** — "keep everything from the left side." The COALESCE pattern for
  handling NULLs in the right side is a practical must-know.
- **FULL OUTER JOIN** — reconciliation use case.
- **CROSS JOIN** — Cartesian product. We deliberately use small derived tables
  to avoid a combinatorial explosion.
- **SEMI JOIN / ANTI JOIN** — DuckDB supports these as first-class syntax
  (many databases require EXISTS/NOT EXISTS instead). Showing the cleaner syntax
  is a DuckDB selling point.
- **Self JOIN** — "customer pairs who bought the same product." The
  `s1.customer < s2.customer` trick for avoiding duplicates is a classic pattern.
- **JOIN + GROUP BY** — the bread-and-butter analytics combo.

### 24 — Subqueries & CTEs

After learning aggregation and JOINs, learners need tools to **compose** queries.
This example introduces layered thinking:

- **Scalar subqueries** — a subquery that returns one value. Used in SELECT and
  WHERE. The "compare each row to the average" pattern appears in every data
  analysis task.
- **IN subquery** — filter against a derived list. More flexible than a hardcoded
  IN ('a', 'b') list.
- **EXISTS** — the boolean existence check. Important for understanding semi-join
  semantics.
- **Correlated subqueries** — the inner query references the outer row. We
  explain the "runs once per row" mental model while noting DuckDB optimizes it.
- **CTEs (WITH clause)** — the main event. CTEs are the single most important
  readability tool in analytical SQL. We show single CTEs, chained CTEs, and then
  a direct CTE-vs-nested-subquery comparison so the readability advantage is
  undeniable.

### 25 — CASE expressions

CASE is SQL's `if/else`. After learning to filter, aggregate, join, and compose
queries, learners need **conditional logic** to handle real-world data:

- **Searched CASE WHEN** — the general form. Order-size classification is a
  relatable business example.
- **Simple CASE** — shorthand for equality comparisons.
- **COALESCE** — the NULL-handling workhorse. Every production query uses it.
- **NULLIF** — the lesser-known sibling. The divide-by-zero avoidance pattern
  (`100.0 / NULLIF(x, 0)`) is essential.
- **DuckDB's IF()** — a non-standard but very convenient ternary function.
  Showing it after the standard CASE teaches the concept before the shortcut.
- **Conditional aggregation** — CASE inside SUM/COUNT. This is the manual
  approach; example 22 showed the FILTER clause alternative.
- **CASE in ORDER BY** — custom sort priorities. A common real-world need
  (e.g., "show VIP customers first").
- **Nested CASE** — included to show it's possible, with a note that CTEs are
  often more readable.

### 26 — Set operations

Set operations round out Core SQL by teaching how to **combine result sets**:

- **UNION ALL** — the workhorse. Most real pipelines use UNION ALL (not UNION)
  because they know their data is already distinct or they want duplicates.
- **UNION** — deduplication. We add a region tag to show when dedup matters.
- **INTERSECT** — finding overlap between two datasets.
- **EXCEPT** — finding differences. We show both directions (A EXCEPT B and
  B EXCEPT A) to drive home that order matters.
- **UNION BY NAME** — DuckDB's schema-flexible union. This is a killer feature
  for combining tables with slightly different schemas (extra columns become
  NULL). Most databases don't support this.
- **Set ops + aggregation** — a practical CTE pattern that combines INTERSECT
  with per-region revenue totals.
- **INTERSECT ALL / EXCEPT ALL** — brief mention with a counting comparison to
  show they exist.

The example uses inline data (two regional sales tables) rather than the bundled
CSV, so the overlapping rows are obvious by inspection.

## Design decisions

- **Reuse sales.csv and products.jsonl from Track 1** in examples 21-25. Familiar
  data reduces cognitive load — the learner focuses on SQL, not data comprehension.
- **Example 26 uses inline data** because set operations need controlled overlap
  between two tables, which is hard to demonstrate with a single CSV.
- **Each example is independent** — you can run 25 without having run 21-24.
- **"Show the wrong way first" guideline** from the SKILL.md is applied in
  example 24 (nested subquery vs CTE comparison) and example 22 (WHERE vs HAVING
  distinction).
- **DuckDB-specific features are highlighted** (GROUP BY ALL, FILTER clause,
  SEMI/ANTI JOIN syntax, UNION BY NAME, IF()) but always *after* the standard
  SQL equivalent is shown, so learners understand both.
