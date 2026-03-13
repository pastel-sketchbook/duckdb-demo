---
name: duckdb-learning
version: 1.0.0
description: |
  Comprehensive DuckDB + Python learning skill organized into 8 progressive
  tracks -- from first query to interactive charts.  Each skill maps to a
  self-contained example script in `examples/`.  Use this skill when building
  new examples, reviewing coverage gaps, or guiding a learner through the
  DuckDB analytics stack.
allowed-tools:
  - Read
  - Grep
  - Glob
  - Bash
  - Task
  - WebFetch
  - TodoWrite
  - Edit
  - Write
---

# DuckDB Learning Skills

8 progressive tracks, 42 skills, each mapped to an example script.
Skills marked [DONE] already have an example in the repo.

Reference: https://duckdb.org/docs/stable/

---

## Track 1 -- Foundations

Get data in, run your first queries, zero infrastructure.

| #  | Skill | Example file | Key concepts |
|----|-------|-------------|--------------|
| 11 | Quick-start CSV query | `11_quick_start.py` [DONE] | `duckdb.sql()`, `read_csv()`, `GROUP BY`, `SUM` |
| 12 | Excel to DuckDB | `12_excel_to_duckdb.py` [DONE] | `INSTALL`/`LOAD excel`, `read_xlsx()`, persistent `.duckdb` file |
| 13 | Read Parquet files | `13_read_parquet.py` [DONE] | `read_parquet()`, column pruning, row-group filtering, metadata inspection |
| 14 | Read JSON files | `14_read_json.py` [DONE] | `read_json()`, `read_json_auto()`, newline-delimited JSON, nested fields |
| 15 | Create tables & insert data | `15_create_and_insert.py` [DONE] | `CREATE TABLE`, `INSERT INTO`, `CREATE TABLE AS`, `DESCRIBE`, data types |

### How to build a Foundations example

```text
1. Use `duckdb.connect()` (in-memory) unless persistence is the point.
2. Point at a file in `src/duckdb_demo/data/` or create sample data inline.
3. Show the raw data first (SELECT * ... LIMIT 5), then demonstrate the skill.
4. Keep SQL strings readable -- use triple-quoted f-strings.
5. Print section headers with `=== Title ===` for scannable output.
```

---

## Track 2 -- Core SQL

The everyday SQL toolkit every analyst needs.

| #  | Skill | Example file | Key concepts |
|----|-------|-------------|--------------|
| 21 | Filtering & sorting | `21_filter_and_sort.py` | `WHERE`, `ORDER BY`, `LIMIT`, `OFFSET`, `BETWEEN`, `IN`, `LIKE` |
| 22 | Aggregations | `22_aggregations.py` | `GROUP BY`, `HAVING`, `SUM`, `COUNT`, `AVG`, `MIN`, `MAX`, `COUNT(DISTINCT)` |
| 23 | JOINs | `23_joins.py` | `INNER JOIN`, `LEFT JOIN`, `FULL OUTER`, `CROSS JOIN`, `SEMI JOIN`, `ANTI JOIN` |
| 24 | Subqueries & CTEs | `24_subqueries_ctes.py` | `WITH` (CTE), scalar subquery, correlated subquery, `EXISTS` |
| 25 | CASE expressions | `25_case_expressions.py` | `CASE WHEN`, `COALESCE`, `NULLIF`, `IF`, conditional aggregation |
| 26 | Set operations | `26_set_operations.py` | `UNION`, `UNION ALL`, `INTERSECT`, `EXCEPT` |

### How to build a Core SQL example

```text
1. Use the bundled sales.csv or inventory.xlsx -- familiar data reduces
   cognitive load for learners.
2. Show the "wrong" or naive approach first, then the cleaner SQL version.
3. Add a comment above every SQL block explaining *why* this clause matters.
4. For JOINs, create a second small dataset so the join is meaningful.
```

---

## Track 3 -- Intermediate SQL

Window functions, dates, strings, and DuckDB's unique SQL extensions.

| #  | Skill | Example file | Key concepts |
|----|-------|-------------|--------------|
| 31 | Window functions -- ranking | `31_window_ranking.py` | `ROW_NUMBER()`, `RANK()`, `DENSE_RANK()`, `NTILE()`, `PARTITION BY` |
| 32 | Window functions -- aggregates | `32_window_aggregates.py` | Running totals, moving averages, `LAG()`, `LEAD()`, frame clauses (`ROWS BETWEEN`) |
| 33 | Date & time functions | `33_dates_and_times.py` | `date_trunc`, `date_part`, `date_diff`, `date_add`, `INTERVAL`, `strftime` |
| 34 | String functions & regex | `34_strings_and_regex.py` | `LIKE`, `ILIKE`, `regexp_matches`, `regexp_extract`, `string_split`, `concat` |
| 35 | PIVOT & UNPIVOT | `35_pivot_unpivot.py` | `PIVOT ... ON ... USING`, `UNPIVOT`, reshaping data for reporting |
| 36 | Friendly SQL extensions | `36_friendly_sql.py` | `QUALIFY`, `SAMPLE`, `EXCLUDE`, `REPLACE`, `COLUMNS()`, `GROUP BY ALL`, `ORDER BY ALL` |

### How to build an Intermediate SQL example

```text
1. Start with a "real question" (e.g., "Who was the top customer each month?")
   then show how the SQL feature answers it.
2. For window functions, print intermediate results so the learner can see
   how the window frame moves.
3. For PIVOT/UNPIVOT, show before & after tables side by side.
```

---

## Track 4 -- Data I/O & Export

Moving data between formats and locations.

| #  | Skill | Example file | Key concepts |
|----|-------|-------------|--------------|
| 41 | Export to CSV & Parquet | `41_export_formats.py` [DONE] | `COPY ... TO`, `.write_parquet()`, `.write_csv()`, format options |
| 42 | Multi-file reads (globs) | `42_multi_file_glob.py` [DONE] | `read_csv('data/*.csv')`, `filename` column, `union_by_name` |
| 43 | Hive-partitioned data | `43_hive_partitioning.py` [DONE] | `hive_partitioning = true`, partitioned writes with `PARTITION_BY` |
| 44 | HTTP & remote files | `44_http_remote.py` [DONE] | `httpfs` extension, `read_parquet('https://...')`, reading from URLs |
| 45 | ATTACH & multi-database | `45_attach_databases.py` [DONE] | `ATTACH`, `DETACH`, `USE`, querying across SQLite/Postgres/DuckDB files |

### How to build a Data I/O example

```text
1. For export examples, write to a temp directory and clean up at the end.
2. For remote/HTTP examples, use a small public dataset (DuckDB docs or
   Hugging Face datasets are good sources).
3. Show round-trip: write data out, read it back, verify row counts match.
```

---

## Track 5 -- Python Interop

DuckDB as the SQL engine for your Python data stack.

| #  | Skill | Example file | Key concepts |
|----|-------|-------------|--------------|
| 51 | pandas interop | `51_pandas_interop.py` [DONE] | `duckdb.sql("SELECT * FROM df")`, `.df()`, `.fetchdf()`, zero-copy |
| 52 | polars interop | `52_polars_interop.py` [DONE] | Query Polars frames with SQL, `.pl()`, LazyFrame integration |
| 53 | pyarrow interop | `53_pyarrow_interop.py` [DONE] | `.arrow()`, `.fetchnumpy()`, Arrow tables as virtual DuckDB tables |
| 54 | Relational API | `54_relational_api.py` [DONE] | `.filter()`, `.project()`, `.aggregate()`, `.order()`, method chaining |
| 55 | Python UDFs | `55_python_udfs.py` [DONE] | `conn.create_function()`, scalar UDF, type mapping, error handling |

### How to build a Python Interop example

```text
1. Add the interop library (pandas/polars/pyarrow) via `uv add <pkg>`.
2. Make it crystal clear which library is doing the work at each step --
   use comments like "# DuckDB executes the SQL" vs "# pandas does this".
3. Show the zero-copy advantage: time a query vs doing it purely in pandas.
```

---

## Track 6 -- Advanced SQL

Nested data, full-text search, and DuckDB-specific power features.

| #  | Skill | Example file | Key concepts |
|----|-------|-------------|--------------|
| 61 | Nested types: STRUCT, LIST, MAP | `61_nested_types.py` | `STRUCT`, `LIST`, `MAP`, `unnest()`, struct access with dot notation |
| 62 | JSON processing | `62_json_processing.py` | `json_extract`, `json_structure`, `json_transform`, `json_keys`, JSON type |
| 63 | Recursive CTEs | `63_recursive_ctes.py` | `WITH RECURSIVE`, tree traversal, graph walks, hierarchy flattening |
| 64 | ASOF joins | `64_asof_joins.py` | `ASOF JOIN ... ON ... AND`, time-series alignment, nearest-match joins |
| 65 | GROUPING SETS | `65_grouping_sets.py` | `GROUPING SETS`, `ROLLUP`, `CUBE`, `GROUPING()` function, subtotals |
| 66 | Full-text search | `66_full_text_search.py` | `fts` extension, `PRAGMA create_fts_index`, `fts_main_docs.match_bm25()` |

### How to build an Advanced SQL example

```text
1. These skills deserve a longer preamble explaining *when* you would use
   them in real analytics work.
2. For nested types and JSON, create inline data with DuckDB's struct/list
   literals -- no external file needed.
3. For ASOF joins, generate two time-series datasets with slightly
   different timestamps to show the "nearest" matching behavior.
```

---

## Track 7 -- Visualization

Query with DuckDB, chart with Python.

| #  | Skill | Example file | Key concepts |
|----|-------|-------------|--------------|
| 71 | matplotlib bar & line charts | `71_matplotlib_charts.py` | `.df()` to pandas, `plt.bar()`, `plt.plot()`, `plt.savefig()` |
| 72 | plotly interactive charts | `72_plotly_charts.py` | `plotly.express`, `.df()`, interactive HTML output, `fig.write_html()` |
| 73 | seaborn statistical plots | `73_seaborn_plots.py` | `sns.barplot()`, `sns.heatmap()`, `.df()` for seaborn integration |
| 74 | Chart-ready data with PIVOT | `74_chart_ready_data.py` | `PIVOT` for wide-format, `UNPIVOT` for long-format, export for BI tools |

### How to build a Visualization example

```text
1. Add the viz library (matplotlib/plotly/seaborn) via `uv add <pkg>`.
2. The SQL query should do the heavy lifting -- aggregation, date
   truncation, pivoting -- so the Python plot code is minimal.
3. Save charts to `examples/output/` (add that dir to .gitignore).
4. Use `plt.savefig()` / `fig.write_html()` so the example works headless
   (no GUI required).
5. Print a message telling the user where the chart was saved.
```

---

## Track 8 -- Real-World Patterns

Patterns you will use on every analytics project.

| #  | Skill | Example file | Key concepts |
|----|-------|-------------|--------------|
| 81 | ETL pipeline | `81_etl_pipeline.py` | Read mixed sources, transform in SQL, write Parquet, verify output |
| 82 | Data quality checks | `82_data_quality.py` | `SUMMARIZE`, `NULL` detection, `CHECK` constraints, outlier flagging |
| 83 | Deduplication | `83_deduplication.py` | `ROW_NUMBER() OVER (...) QUALIFY`, `DISTINCT ON`, duplicate detection |
| 84 | Synthetic data generation | `84_synthetic_data.py` | `generate_series()`, `range()`, `random()`, `uuid()`, test data creation |
| 85 | Query profiling | `85_query_profiling.py` | `EXPLAIN`, `EXPLAIN ANALYZE`, `PRAGMA enable_profiling`, reading query plans |

### How to build a Real-World Patterns example

```text
1. Frame the example around a realistic scenario (e.g., "your data
   warehouse has duplicate customer records...").
2. For ETL, combine at least two source formats (CSV + JSON, Excel +
   Parquet) to show DuckDB's multi-format strength.
3. For profiling, show a "slow" query and an optimized version, explain
   how EXPLAIN ANALYZE reveals the difference.
```

---

## Example template

Every example script should follow this structure:

```python
"""<One-line description of what this example teaches.>

Demonstrates how to:
  1. <First concept>
  2. <Second concept>
  3. <Third concept>

Run with:
    uv run python examples/NN_script_name.py
"""

from __future__ import annotations

from pathlib import Path

import duckdb

# -- Paths ----------------------------------------------------------------
DATA_DIR = Path(__file__).resolve().parent.parent / "src" / "duckdb_demo" / "data"

# -- 1. Section title -----------------------------------------------------
print("=== Section Title ===")
# ... demo code ...

# -- 2. Next section -------------------------------------------------------
print("=== Next Section ===")
# ... demo code ...
```

## Dependencies by track

| Track | Required packages | Install command |
|-------|------------------|----------------|
| 1-4, 6, 8 | `duckdb` (+ `openpyxl` for Excel) | already installed |
| 5 - pandas | `pandas` | `uv add pandas` |
| 5 - polars | `polars` | `uv add polars` |
| 5 - pyarrow | `pyarrow` | `uv add pyarrow` |
| 7 - matplotlib | `matplotlib` | `uv add matplotlib` |
| 7 - plotly | `plotly` | `uv add plotly` |
| 7 - seaborn | `seaborn` | `uv add seaborn` |

## DuckDB extensions used

| Extension | Tracks | Install |
|-----------|--------|---------|
| `excel` | 1 | `INSTALL excel; LOAD excel;` |
| `httpfs` | 4 | `INSTALL httpfs; LOAD httpfs;` |
| `fts` | 6 | `INSTALL fts; LOAD fts;` |
| `icu` | 3 (dates/tz) | `INSTALL icu; LOAD icu;` |
| `json` | 1, 6 | auto-loaded |

## Progress tracker

Update this section as examples are completed:

```
Track 1 - Foundations ........... [5/5]  ██████████
Track 2 - Core SQL ............. [6/6]  ██████████
Track 3 - Intermediate SQL ..... [6/6]  ██████████
Track 4 - Data I/O & Export .... [5/5]  ██████████
Track 5 - Python Interop ....... [5/5]  ██████████
Track 6 - Advanced SQL ......... [0/6]  ░░░░░░░░░░
Track 7 - Visualization ........ [0/4]  ░░░░░░░░░░
Track 8 - Real-World Patterns .. [0/5]  ░░░░░░░░░░
                                  ─────
                          Total  27/42
```
