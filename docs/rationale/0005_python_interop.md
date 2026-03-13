# Track 5 — Python Interop: Rationale

## Intention

Track 5 shows DuckDB as the **SQL engine for your Python data stack**. The
learner already knows how to query files and write SQL; now they learn to move
data fluidly between DuckDB and the three dominant Python data libraries
(pandas, polars, pyarrow), build queries programmatically with the Relational
API, and extend DuckDB with custom Python functions.

The key insight this track instills: DuckDB is not a replacement for pandas or
polars — it is a **complement**. Use DuckDB for heavy SQL (aggregation, joins,
windows), then hand off to your favourite library for visualization, ML, or
domain logic.

## Why these five examples, in this order

### 51 — pandas interop

pandas is the most widely used Python data library. Most learners already know
it, so starting here minimises the "new thing" count — the only new concept is
that DuckDB can query a pandas DataFrame by variable name.

- **Query a DataFrame with SQL** is the "aha!" moment. `duckdb.sql("SELECT *
  FROM df")` looks like magic but is just DuckDB scanning the DataFrame via
  Apache Arrow under the hood.
- **.df() and .fetchdf()** are shown as the two ways to get back to pandas.
  They're functionally identical; showing both prevents future confusion.
- **CSV → DuckDB → pandas** is the practical pipeline: DuckDB reads the file
  (faster than `pd.read_csv`), transforms it with SQL, and hands a DataFrame
  to the learner for further work.
- **SQL aggregation on a DataFrame** demonstrates that DuckDB's SQL engine is
  available even when your data starts in pandas — no need to create a table
  first.
- **Round-trip** (pandas → DuckDB table → query → pandas) establishes the
  full lifecycle.
- **Zero-copy note** plants the seed for the Arrow-based architecture that
  example 53 explores in depth.

### 52 — polars interop

polars is the rising alternative to pandas with better performance and a
cleaner API. The structure mirrors example 51 deliberately so the learner
can compare the two ecosystems.

- **.pl()** is the polars equivalent of `.df()`. Simple, symmetric.
- **LazyFrame integration** is the polars-specific feature worth highlighting.
  DuckDB can query a LazyFrame directly, which means you can compose a lazy
  computation in polars and hand it to DuckDB for the SQL step.
- **Native polars vs DuckDB SQL** comparison at the end helps the learner
  decide when to use which tool: polars for method-chaining workflows, DuckDB
  for complex SQL (CTEs, window functions, multi-source joins).

### 53 — pyarrow interop

PyArrow is the interchange layer that makes pandas, polars, and DuckDB play
together. This example demystifies Arrow and shows it as the universal data bus.

- **.arrow()** converts DuckDB results to an Arrow table — the most efficient
  format for further processing.
- **.fetchnumpy()** provides a numpy-native escape hatch for ML/scientific
  workflows that don't need DataFrames.
- **Arrow as interchange** (DuckDB → Arrow → pandas AND polars) is the key
  architectural insight: Arrow is the zero-copy bridge.
- **Zero-copy demonstration** with a 1M-row table makes the performance
  advantage tangible rather than theoretical.

### 54 — Relational API

The Relational API is DuckDB's programmatic query builder — an alternative to
SQL strings for cases where queries need to be composed dynamically.

- **Method-by-method walkthrough** (.filter, .project, .aggregate, .order,
  .limit) maps each method to its SQL equivalent, building understanding
  incrementally.
- **Method chaining** shows the fluid, builder-pattern style that feels
  natural to Python developers.
- **Side-by-side comparison** at the end shows the same query written as SQL
  and as Relational API calls, producing identical results. This helps the
  learner choose the right approach for each situation: SQL for readability,
  Relational API for dynamic composition.

### 55 — Python UDFs

User-Defined Functions let you extend DuckDB with arbitrary Python logic.
This is the "escape hatch" when SQL alone isn't enough.

- **Simple scalar UDF** (Celsius → Fahrenheit) is the minimal example.
- **String UDF** (slugify) shows a real use case: Python's stdlib (regex) is
  available inside a SQL query.
- **Type mapping table** (str→VARCHAR, int→INTEGER, etc.) is essential
  reference material for anyone writing UDFs.
- **UDF on real data** (pricing tiers on sales.csv) connects the abstract
  concept to the learner's familiar dataset.
- **NULL handling** covers the subtle default (function skipped, NULL returned)
  and the explicit override (`FunctionNullHandling.SPECIAL`).
- **Error handling** shows both the default (exception aborts query) and the
  lenient mode (`PythonExceptionHandling.RETURN_NULL`).
- **Limitations** are stated honestly: UDFs are slow (Python function call per
  row), not persisted, and limited in type support.

## Design decisions

- **Mirror structure across 51/52** — pandas and polars examples follow the
  same section order so the learner can compare them directly. The differences
  (`.df()` vs `.pl()`, LazyFrame, method-chaining style) stand out against the
  shared structure.

- **Arrow as the bridge in 53** — this example is placed third because the
  learner needs to see pandas and polars first to appreciate what Arrow
  connects. It's the "why" behind the zero-copy magic hinted at in 51 and 52.

- **Relational API before UDFs** — the Relational API is a pure DuckDB
  feature (no external libraries). Placing it before UDFs keeps the "pure
  DuckDB" and "extend DuckDB" boundary clear.

- **UDFs last** — UDFs are the most advanced topic in this track. They require
  understanding of DuckDB's type system, function registration, and error
  semantics. Placing them last means the learner has maximum context.

- **No benchmarking** — the skills roadmap suggested timing queries, but
  microbenchmarks in demo scripts are unreliable and confusing for beginners.
  Instead, we state the zero-copy and performance characteristics in comments
  and let the learner measure in their own workloads.
