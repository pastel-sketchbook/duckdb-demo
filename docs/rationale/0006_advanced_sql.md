# Track 6 — Advanced SQL: Rationale

## Intention

Track 6 explores DuckDB's power features — the capabilities that go beyond
standard analytics SQL into territory usually reserved for specialised systems
(document stores, graph databases, search engines). A learner who completes
this track can handle nested data, recursive hierarchies, time-series alignment,
multi-dimensional reporting, and full-text search — all within DuckDB.

These are "reach for when you need them" features. They won't appear in every
query, but when the problem calls for them, they eliminate the need for external
tools.

## Why these six examples, in this order

### 61 — Nested types: STRUCT, LIST, MAP

Nested types are foundational for everything else in this track. JSON processing
(62) relies on STRUCTs and LISTs; unnest() appears in recursive CTEs (63);
list functions show up in real-world patterns (Track 8).

- **STRUCT** comes first because it's the simplest nested type — a fixed set
  of named fields, like a row within a row.
- **LIST** follows naturally — a variable-length array of values.
- **MAP** is shown third as a key-value extension of the struct concept.
- **unnest()** gets its own section because it's the most common operation on
  nested data. Exploding a list into rows is how you bridge nested and flat
  representations.
- **Nested nesting** (STRUCT containing LIST, LIST of STRUCTs) pushes the
  concept further and prepares the learner for real-world JSON structures.
- **List functions** (list_filter, list_transform, list_aggregate) show that
  DuckDB can process lists without unnesting — lambda-style operations that
  feel more like Python than SQL.
- **Practical example** (order with line items) ties everything together in
  a denormalization/renormalization scenario that mirrors real data warehouse
  patterns.

### 62 — JSON processing

JSON is the lingua franca of APIs, logs, and semi-structured data. This example
builds directly on the nested types from 61.

- **json_structure()** comes first because understanding the shape of your JSON
  is step zero. Without it, you're guessing at paths.
- **json_extract / json_extract_string** with the `->` and `->>` shorthand
  operators are the daily-driver functions.
- **json_keys** enables schema discovery — essential for unfamiliar data.
- **json_transform** is DuckDB's bulk-casting superpower: one call converts
  an entire JSON structure to typed columns.
- **JSON arrays** get special attention because array processing (filtering,
  unnesting) is where most JSON complexity lives.
- **Full ETL transformation** (JSON → clean table) shows the end-to-end
  pipeline that a real data engineer would build.
- **Creating JSON from SQL** (to_json, json_object, json_array) rounds out
  the bi-directional capability.

Design note: the file reads JSONL as raw text via `read_csv(sep=chr(0))` then
casts to JSON. This gives full control over extraction and avoids DuckDB's
`read_json()` auto-flattening, which hides the raw JSON that the learner needs
to see.

### 63 — Recursive CTEs

Recursive CTEs are the SQL answer to tree and graph problems. They're
conceptually challenging, so the progression is carefully graded.

- **Number sequence (1..10)** is the minimal recursive CTE. It has no joins,
  no trees — just a base case and a recursive step. This removes all
  distractions and focuses on the WITH RECURSIVE mechanics.
- **Org chart** is the canonical tree example. Everyone understands
  employee-manager relationships.
- **Path building** extends the org chart to construct breadcrumb strings,
  which is the most common real-world recursive CTE output.
- **Bill of materials** introduces arithmetic in recursion — multiplying
  quantities through levels — which is a classic manufacturing/supply-chain
  problem.
- **Graph walk** generalises from trees to graphs, showing reachability and
  shortest hop count.
- **Hierarchy flattening** is the practical output format: a flat table with
  level, root, and breadcrumb columns ready for reporting.
- **Cycle safety** closes with the most important production concern: why
  UNION (not UNION ALL) prevents infinite loops in cyclic data.

### 64 — ASOF joins

ASOF joins solve the "nearest match" problem in time-series data. They're
niche but indispensable for financial, IoT, and event-based analytics.

- **Stock trades + quotes** is the textbook setup. Two time-series with
  deliberately misaligned timestamps make the "nearest match" visually obvious.
- **Basic ASOF JOIN (>=)** matches each trade to the most recent quote — the
  backwards-looking pattern that's most common in practice.
- **ASOF vs regular JOIN** demonstrates concretely why regular joins fail when
  timestamps don't match exactly (zero rows returned).
- **Forward-looking (<=)** flips the inequality, showing that ASOF works in
  both temporal directions.
- **Event attribution** applies the same pattern to a marketing scenario
  (match events to campaigns), proving ASOF isn't finance-specific.
- **Multiple ASOF joins** chains two joins to enrich trades with both bid and
  ask prices — a realistic multi-source enrichment pattern.

### 65 — GROUPING SETS

GROUPING SETS, ROLLUP, and CUBE produce subtotals and grand totals in a single
query pass. This is a reporting feature that eliminates verbose UNION ALL.

- **The naive approach** (UNION ALL of multiple GROUP BYs) is shown first so
  the learner feels the pain before seeing the solution.
- **GROUPING SETS** gives explicit control — the learner specifies exactly
  which grouping combinations to compute.
- **ROLLUP** produces hierarchical subtotals (the common case for business
  reports: total → by-month → by-product).
- **CUBE** produces all combinations — useful for cross-tabulation.
- **GROUPING() function** solves the NULL ambiguity: is this NULL because the
  data is NULL, or because it's a subtotal row?
- **Practical report** combines everything into a labeled sales report with
  DETAIL, SUBTOTAL, and GRAND TOTAL row types — output that could go directly
  to a stakeholder.

### 66 — Full-text search

FTS rounds out the track by showing DuckDB as a lightweight search engine.
This is the most "surprising" capability — most people don't expect an
analytical database to do BM25 ranking.

- **Extension installation** is explicit because `fts` isn't auto-loaded.
- **Document creation** uses 10 tech-topic documents with enough text (2-3
  sentences each) to make BM25 scoring meaningful.
- **PRAGMA create_fts_index** is DuckDB's FTS interface — unconventional but
  straightforward.
- **match_bm25()** is the core search function. Single-term and multi-term
  queries show how relevance scoring works.
- **Column-specific indexing** (title-only vs title+body) shows that you can
  control what gets indexed.
- **FTS + SQL filters** combines search with WHERE clauses — the real-world
  pattern where you search within a category or date range.
- **Stale index warning** is critical: FTS indexes don't auto-update. The
  example demonstrates the problem (insert → search → 0 rows) and the
  solution (drop + recreate).

## Design decisions

- **All data inline** for 61, 63, 64, 66 — these examples need specific data
  shapes (trees, time-series, documents) that the bundled sales.csv can't
  provide. Inline VALUES keeps examples self-contained.

- **sales.csv reused** in 65 — GROUPING SETS is a reporting feature, and the
  sales data is the right fit for a business report with subtotals.

- **products.jsonl reused** in 62 — the bundled JSON file has the nested
  structures (specs object, tags array) that JSON processing needs.

- **Progressive complexity** — the track opens with type primitives (61), uses
  them in JSON (62), introduces recursion (63), then specialised joins (64),
  aggregation extensions (65), and search (66). Each example builds on
  concepts from earlier in the track.

- **No external dependencies** — unlike Track 5, this track uses only DuckDB.
  The fts extension is built-in (INSTALL/LOAD), not a Python package.
