---
description: DuckDB demo for novice AI/BI developers using uv, ruff, and ty.
globs: "*.py, *.yaml, *.toml, *.sql, *.csv, *.parquet"
alwaysApply: true
---

## Project Overview

A hands-on DuckDB + Python demo aimed at **novice AI/BI developers**. The goal is to teach practical data querying, transformation, and analysis skills using DuckDB's in-process SQL engine -- no database server required. Every example should be self-contained, well-commented, and easy to follow for someone new to the data stack.

## Audience

- Beginners learning SQL-based analytics in Python
- AI/BI developers exploring local-first, serverless analytics
- Anyone evaluating DuckDB as a lightweight alternative to Spark, pandas-only workflows, or traditional databases

Keep code simple, add comments liberally, and prefer clarity over cleverness.

## Toolchain

Use **uv** as the sole package manager and task runner. Do not use pip, poetry, pipenv, or conda.

- Use `uv add <pkg>` to add dependencies (not `pip install`)
- Use `uv run <cmd>` to run commands within the project venv
- Use `uv sync` to install/update the lockfile after editing `pyproject.toml`
- Use `uv init` for new projects
- Dev dependencies go in `[dependency-groups] dev` in `pyproject.toml`

Key dependencies: `duckdb`, and optionally `pandas`, `pyarrow`, `polars` when demonstrating interop.

## Pre-commit Requirement

**IMPORTANT:** Always run these checks before committing:

```sh
uv run ruff check .          # lint
uv run ruff format --check . # format check
uv run ty check              # type check
```

All three must pass. To auto-fix lint and formatting: `uv run ruff check --fix . && uv run ruff format .`

## Code Style

- **Linter/Formatter:** ruff (configured in `pyproject.toml` under `[tool.ruff]`)
- **Type checker:** ty (configured under `[tool.ty.environment]`)
- Line length: 100
- Target: Python 3.10+
- Lint rules: E, F, I (isort), UP (pyupgrade)
- Use type hints on all function signatures
- Prefer `from __future__ import annotations` for modern annotation syntax when needed

## Project Layout

```
src/duckdb_demo/       # Application package (src layout)
  __init__.py
  main.py              # Entry point / CLI runner
  queries.py           # Reusable SQL queries and helper functions
  data/                # Sample CSV/Parquet/JSON datasets
examples/              # Standalone example scripts (one concept per file)
pyproject.toml
```

Source code lives under `src/duckdb_demo/`. Follow the existing src layout -- do not place modules at the project root. Standalone example scripts go in `examples/`.

## DuckDB Conventions

- Use `duckdb.connect()` (in-memory) for demos unless persistence is explicitly needed
- For persistent databases, use `duckdb.connect("path/to/file.duckdb")`
- Prefer DuckDB SQL over Python-side data manipulation when demonstrating DuckDB capabilities
- Show both the SQL string and the Python API usage side by side where it aids learning
- Use `duckdb.sql()` for quick one-off queries; use `conn.execute()` / `conn.sql()` when working with a connection object
- Demonstrate reading directly from CSV/Parquet/JSON files (DuckDB's native file scanning) -- this is a key differentiator
- When showing pandas/polars interop, make it clear which library is doing the work

## Skills Roadmap

The full learning roadmap lives in `.claude/skills/duckdb-learning/SKILL.md`.
It defines **8 tracks / 42 skills**, each mapped to an example script:

1. **Foundations** -- connect, read CSV/Excel/Parquet/JSON, create tables
2. **Core SQL** -- filtering, aggregations, JOINs, CTEs, CASE, set operations
3. **Intermediate SQL** -- window functions, dates, strings, PIVOT, QUALIFY
4. **Data I/O & Export** -- COPY TO, globs, Hive partitioning, httpfs, ATTACH
5. **Python Interop** -- pandas, polars, pyarrow, Relational API, UDFs
6. **Advanced SQL** -- nested types, JSON, recursive CTEs, ASOF, FTS
7. **Visualization** -- matplotlib, plotly, seaborn, chart-ready data
8. **Real-World Patterns** -- ETL, data quality, dedup, synthetic data, profiling

When adding a new example, consult the skill file for the template, naming
convention (`NN_skill_name.py`), and section-building guidelines.  Update
the progress tracker in the skill file when an example is completed.

## Running

```sh
uv sync                                # install dependencies
uv run python -m duckdb_demo.main      # run the main demo
uv run python examples/<script>.py     # run a standalone example
```
