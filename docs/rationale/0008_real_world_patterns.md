# Track 8 — Real-World Patterns: Rationale

## Intention

Track 8 is the capstone. Tracks 1-7 teach individual DuckDB skills in
isolation; Track 8 combines them into patterns that appear in every analytics
project. The learner should finish this track thinking "I can build a real
data pipeline with just DuckDB and Python — no Spark, no Airflow, no
warehouse server."

Each example models a scenario that a junior data engineer or BI developer
will encounter in their first weeks on the job: loading messy multi-format
data, validating it, deduplicating it, generating test data when production
data isn't available, and understanding why a query is slow.

## Why these five examples, in this order

### 81 — ETL Pipeline

ETL is the bread and butter of data engineering. This example demonstrates
the full lifecycle: extract from CSV + JSON, transform with SQL (clean,
enrich via JOINs, aggregate), load to Parquet, and verify round-trip
integrity.

- **Multi-format extraction** — `read_csv()` and `read_json()` in the same
  pipeline shows DuckDB's format-agnostic design. No separate connectors or
  adapters needed.
- **Transform phases** are explicit: clean (TRIM, UPPER, COALESCE), enrich
  (LEFT JOIN to dimension table), aggregate (GROUP BY with window functions).
  Separating these into named steps teaches the ETL mental model.
- **Load to Parquet** via `COPY TO ... (FORMAT PARQUET)` produces a portable
  columnar file. The verification step re-reads the Parquet and compares row
  counts, proving the pipeline is lossless.
- **Temp directory output** keeps the example self-contained — no persistent
  files to clean up.

This is intentionally first because it ties together skills from every prior
track: CSV/JSON reading (Track 1), JOINs and aggregations (Track 2), window
functions (Track 3), COPY TO (Track 4), and `.df()` interop (Track 5).

### 82 — Data Quality Checks

Real data is dirty. Before any analysis, you need to know how dirty. This
example builds a systematic quality framework using only DuckDB SQL.

- **SUMMARIZE** is DuckDB's instant profiling command — one line gives you
  min, max, nulls, quantiles for every column. It's the first thing to run
  on any new dataset.
- **NULL checks** count missing values per column and compute completeness
  percentages. The per-row NULL detection (`WHERE ... IS NULL`) helps triage
  which rows need attention.
- **Duplicate detection** uses GROUP BY + HAVING to find exact duplicates,
  and ROW_NUMBER + QUALIFY to flag them — a bridge to the deeper treatment
  in example 83.
- **Range and outlier checks** use CASE expressions to flag invalid values
  (negative quantities, prices over a threshold, future dates). Row-level
  flags make it easy to export a "quality issues" report.
- **Referential integrity** uses ANTI JOIN to find orphan foreign keys — a
  check that's essential before any JOIN-based analysis.
- **Quality score** — a unified report that aggregates all checks into a
  single percentage per dimension (completeness, validity, referential
  integrity, uniqueness). This teaches the learner to think about data
  quality as a measurable, reportable metric.

### 83 — Deduplication

Deduplication is ordered after quality checks because in practice you
discover duplicates during profiling and then need a systematic strategy
to remove them.

- **Exact duplicates** via GROUP BY + HAVING is the simplest detection
  method. The learner sees it first to build intuition.
- **ROW_NUMBER() + QUALIFY** is the workhorse pattern. Partition by the
  business key, order by the tiebreaker (latest ID, newest date), and
  QUALIFY rn = 1 to keep only the winner. This is a DuckDB-specific
  feature that eliminates the subquery needed in other databases.
- **DISTINCT ON** is DuckDB's PostgreSQL-compatible shorthand. Showing it
  immediately after ROW_NUMBER + QUALIFY lets the learner see that both
  produce the same result, and choose the cleaner syntax when appropriate.
- **Fuzzy deduplication** uses LOWER(TRIM(...)) normalization to reveal
  hidden near-duplicates ("Alice Smith" vs "alice smith" vs " Alice Smith ").
  Jaro-Winkler similarity is also demonstrated for name matching.
- **Audit trail** — the rejected rows are preserved with a rejection reason
  (exact_duplicate or near_duplicate). This is critical in production: you
  need to explain to stakeholders which rows were removed and why.
- **Before/after verification** confirms that kept + rejected = original,
  and that the deduplicated dataset has the expected number of unique keys.

### 84 — Synthetic Data Generation

This example comes after quality and dedup because the learner now
understands what "good data" looks like and can appreciate why generating
controlled test data is valuable.

- **generate_series() / range()** are DuckDB's row generators. They replace
  the need for a Python loop or numpy arange — the rows are generated
  inside the SQL engine.
- **random() + setseed()** produces reproducible random values. The
  reproducibility check (same seed → same output) teaches a concept that
  matters for testing and CI pipelines.
- **uuid()** generates unique identifiers — essential for synthetic primary
  keys that won't collide with production data.
- **Categorical sampling** uses array indexing with `(random() * n)::int`
  to pick from a list of values. This is how you generate realistic
  categorical columns (customer names, product types, statuses) without
  external libraries.
- **Date generation** combines generate_series with date arithmetic to
  create date spines (every day, every month) and random dates within a
  range. Date spines are fundamental to time-series analytics — you need
  them to fill gaps where no events occurred.
- **Complete 1000-row dataset** ties everything together: UUID keys,
  categorical sampling, random dates, random quantities and prices. The
  SUMMARIZE at the end proves the data has the expected distributions.
- **Parquet export + verification** round-trips the data to prove it's
  usable downstream.

### 85 — Query Profiling

Profiling is last because it requires understanding all prior SQL patterns
(filters, JOINs, aggregations, window functions) to interpret query plans
meaningfully.

- **EXPLAIN** shows the logical and physical plan without executing. The
  learner sees the plan structure (SEQ_SCAN → FILTER → PROJECTION) and
  learns to read it bottom-up.
- **EXPLAIN ANALYZE** executes the query and annotates each operator with
  actual row counts and timing. Comparing estimated vs actual cardinality
  is the core skill of query optimization.
- **JOIN profiling** shows how DuckDB builds a hash table from the smaller
  relation and probes it with the larger one. Understanding HASH_JOIN
  behavior helps the learner predict when JOINs will be fast or slow.
- **Strategy comparison** profiles the same business question (top product
  per customer) implemented two ways: ROW_NUMBER subquery vs CTE + JOIN.
  EXPLAIN ANALYZE reveals which produces fewer intermediate rows.
- **PRAGMA enable_profiling = 'json'** demonstrates machine-readable
  profiling output. The example parses the JSON to extract per-operator
  timing, showing that profiling data can feed automated performance
  monitoring.

## Design decisions

- **Intentionally messy data** — Examples 82 and 83 create deliberately
  dirty datasets (NULLs, duplicates, negative values, future dates, orphan
  FKs) because clean demo data hides the very problems these techniques
  solve. The learner sees realistic imperfections.

- **Temp directories for output** — Examples 81 and 84 write Parquet files
  to `tempfile.mkdtemp()` rather than polluting the project directory. The
  OS cleans them up. The paths are printed so the learner can inspect the
  files if desired.

- **No external dependencies** — All five examples use only `duckdb` and
  Python's standard library. No pandas, no faker, no great_expectations.
  This reinforces the message that DuckDB alone is sufficient for
  production-grade data engineering patterns.

- **Verification steps** — Every example that produces output (ETL, dedup,
  synthetic data) includes a verification section that proves the output is
  correct (row counts match, types preserved, constraints satisfied). This
  teaches defensive programming habits.

- **10,000-row synthetic dataset in 85** — The profiling example generates
  a larger dataset (10K rows, 50 customers) to make timing differences
  visible in EXPLAIN ANALYZE output. Smaller datasets execute too fast to
  show meaningful operator-level timing.
