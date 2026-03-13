# Track 4 — Data I/O & Export: Rationale

## Intention

Track 4 shifts from *querying* data to *moving* data. Tracks 1-3 assumed data
was already in front of the learner; this track teaches the full read/write
lifecycle — exporting results, ingesting many files at once, partitioning for
performance, reading from the network, and bridging multiple databases.

These are the skills that turn DuckDB from a learning toy into a practical ETL
tool. A learner who completes Track 4 can build real data pipelines without
reaching for Spark, Airflow, or heavyweight infrastructure.

## Why these five examples, in this order

### 41 — Export to CSV & Parquet

The track opens with export because it is the natural follow-up to three tracks
of SELECT queries: "I've built a result set — now how do I save it?"

- **COPY ... TO** is shown first because it is the SQL-native approach, familiar
  to anyone coming from Postgres or MySQL.
- **Format options** (delimiter, header, compression) demonstrate that COPY is
  not a blunt instrument — you control the output precisely.
- **Python API methods** (`.write_csv()`, `.write_parquet()`) are shown second
  so the learner sees both the SQL and the Pythonic way.
- **COPY TO JSON** rounds out the format coverage. JSONL is increasingly the
  default exchange format for web APIs and log pipelines.
- **Round-trip verification** (write, read back, count) reinforces a critical
  habit: always verify your exports. This pattern recurs throughout the track.
- **File size comparison** at the end gives the learner intuition for when
  Parquet's columnar compression pays off vs. CSV's simplicity.

### 42 — Multi-file reads (globs)

After learning to write single files, the learner needs to read *many* files.
Real data arrives in batches — daily CSVs, monthly extracts, partitioned logs.

- **Basic glob** (`*.csv`, `[1-3]`) is the foundational pattern. The bracket
  range syntax is shown early because it's less known than `*`.
- **filename column** solves a constant real-world problem: "which file did
  this row come from?" This is invaluable for debugging data pipelines.
- **Aggregate across files** demonstrates that DuckDB treats globbed files as
  one virtual table — no UNION ALL boilerplate needed.
- **union_by_name** handles the messy reality where files have evolving schemas.
  An extra column (`channel`) in one file is merged gracefully with NULLs.
- **CREATE TABLE from glob** shows materialisation — turning a virtual glob
  into a persistent table for repeated queries.
- **glob() function** for debugging is a practical tip: preview which files
  match before committing to a full scan.

### 43 — Hive-partitioned data

Hive partitioning is the bridge between "files on disk" and "structured data
warehouse." It's how real data lakes organise data for performance.

- **PARTITION_BY write** comes first because understanding the directory
  structure is essential before reading makes sense.
- **Directory tree listing** is printed explicitly so the learner sees the
  `key=value` directory naming convention.
- **hive_partitioning = true** for reads demonstrates DuckDB's auto-detection
  of partition columns from directory names.
- **Partition pruning** is the performance payoff — filtering on the partition
  column skips entire files. This is a "wow" moment for learners used to
  scanning everything.
- **Multi-level partitioning** (product + month) shows how partition depth
  affects directory structure and query flexibility.
- **CSV partitions** prove that Hive partitioning isn't Parquet-only.
- **Append-only pattern** simulates incremental ETL: write one partition at
  a time with OVERWRITE_OR_IGNORE. This is the most common production pattern.

Design note: the PARTITION_BY clause requires the partition column in the
SELECT list — this was a real bug we hit and fixed. The example now includes
`product` in the output columns to keep the COPY happy.

### 44 — HTTP & remote files

DuckDB can query files without downloading them first. This is a powerful
differentiator vs. pandas, where you must `pd.read_csv(url)` into memory.

- **httpfs installation** is shown explicitly because it's not auto-loaded.
- **Remote CSV and Parquet** use real public datasets (DuckDB's own sample
  data) to demonstrate that the syntax is identical to local reads.
- **Column pruning over HTTP** is highlighted because it's the performance
  killer feature: DuckDB only downloads the columns it needs from Parquet,
  not the entire file.
- **try/except wrappers** handle offline environments gracefully. The example
  must be runnable without network access, so a local fallback section
  demonstrates the same query patterns with inline data.
- **Key takeaways** section summarizes the S3/GCS extension path for learners
  who need cloud storage next.

### 45 — ATTACH & multi-database

The track closes with ATTACH because it's the most powerful I/O pattern:
querying across independent databases without copying data.

- **Two separate .duckdb files** are created in setup so the ATTACH has
  something meaningful to connect to.
- **Fully-qualified names** (`db.schema.table`) are shown immediately to
  establish the naming convention.
- **Cross-database JOIN** is the headline feature. Joining sales data with a
  product catalog that lives in a different database file is a real scenario.
- **USE for default switching** reduces verbosity once you're focused on one
  database.
- **Materialise with CTAS** shows how to copy attached data into memory for
  faster repeated queries — important for interactive analysis.
- **DETACH cleanup** demonstrates proper resource management.
- **Local tables survive DETACH** is a subtlety worth showing: data copied
  into memory doesn't disappear when you disconnect the source.

## Design decisions

- **Temp directories everywhere** — every export example writes to
  `tempfile.mkdtemp()` and prints the path. This avoids littering the project
  directory and teaches the learner to use temp dirs for experiments.

- **Round-trip verification** — examples 41 and 43 both read back what they
  wrote and verify row counts. This is a deliberate habit we want to instill.

- **Network resilience in 44** — the HTTP example wraps remote reads in
  try/except so the script runs cleanly offline. This is a pragmatic
  concession: demo code should never crash due to network issues.

- **No S3/GCS examples** — we mention the `aws` and `gcs` extensions in
  passing but don't demo them. They require credentials and cloud
  infrastructure that a beginner won't have. The httpfs example establishes
  the concept; cloud extensions are a natural next step.

- **ATTACH with READ_ONLY** — both attached databases use `(READ_ONLY)` to
  model the safer default. In production, you rarely want ad-hoc writes to
  shared databases.

- **Per-track temp directory prefix** — each example uses a distinct prefix
  (`duckdb_export_`, `duckdb_glob_`, `duckdb_hive_`, `duckdb_attach_`) so
  concurrent runs don't interfere and the learner can find their output.
