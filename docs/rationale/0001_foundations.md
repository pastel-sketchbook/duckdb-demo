# Track 1 — Foundations: Rationale

## Intention

Track 1 exists to eliminate the biggest barrier for beginners: **getting started**.
Most analytics tutorials assume a running database server, schema migrations, and
connection strings. DuckDB removes all of that — you `pip install duckdb` (or
`uv add duckdb`) and start querying files on disk immediately. Track 1 proves
this by progressing through five increasingly rich data formats without ever
requiring a database server.

## Why these five examples, in this order

### 11 — Quick-start CSV query

The very first example must be **zero-friction**. One import, one function call,
real results. We chose `duckdb.sql()` with `read_csv()` because:

- CSV is the universal data exchange format — every learner has seen one.
- `read_csv()` with `auto_detect = true` means no schema definition at all.
- Showing `GROUP BY` + `SUM` in the first 40 lines signals "this is a real
  analytics tool, not a toy."

The example deliberately avoids connection objects. Learners see the shortest
possible path from "I have a CSV" to "I have an answer."

### 12 — Excel to DuckDB

Excel is where most business data lives. Showing `read_xlsx()` early makes
DuckDB immediately relevant to the target audience (AI/BI developers who
receive spreadsheets from stakeholders). This example also introduces:

- **Extensions** (`INSTALL excel; LOAD excel;`) — a concept they will encounter
  again in Tracks 4 and 6.
- **Persistent databases** (`duckdb.connect("file.duckdb")`) — the first time
  data outlives the script. This plants the seed that DuckDB can be more than
  a one-shot query tool.

### 13 — Read Parquet files

Parquet is the de facto columnar format in modern data engineering. After CSV
and Excel, introducing Parquet:

- Shows DuckDB's multi-format superpower (same SQL, different file types).
- Introduces **column pruning** and **row-group filtering** — concepts that
  explain *why* Parquet is faster than CSV for analytical queries.
- The `parquet_metadata()` deep-dive gives learners X-ray vision into the
  file format, building intuition they will need for Tracks 4 (export) and 8
  (profiling).

### 14 — Read JSON files

JSON completes the "big three" file formats. DuckDB's auto-detection of nested
structures (objects → STRUCTs, arrays → LISTs) is a standout feature. This
example:

- Teaches dot-notation access on structs — a natural bridge to Track 6's nested
  types deep-dive.
- Demonstrates `unnest()` for exploding arrays, which is the single most common
  JSON-to-tabular operation.
- Uses JSONL (newline-delimited), which is what most APIs and log systems emit.

### 15 — Create tables & insert data

After four "query files directly" examples, this one shifts perspective: "What
if you want to *define* your own schema?" It covers:

- `CREATE TABLE` with explicit types — the foundation for all later SQL work.
- `INSERT INTO` with single and multi-row VALUES.
- `CREATE TABLE AS` (CTAS) — the pattern used in almost every ETL pipeline.
- `DESCRIBE` and `SHOW TABLES` for introspection.

This example bridges Foundations into Core SQL (Track 2), where every example
will start with a table already loaded.

## Design decisions

- **No imports from `duckdb_demo` package**: every example is self-contained.
  A learner can copy one file and run it in isolation.
- **`DATA_DIR` uses `Path(__file__)`**: portable across machines and working
  directories. Learners don't need to set environment variables.
- **`=== Section Title ===` headers**: provide scannable output when running in
  a terminal. Each section maps to one concept.
- **Comments before every SQL block**: explain *why*, not just *what*. The SQL
  itself is the "what."
