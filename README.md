# duckdb-demo

Hands-on DuckDB + Python demo for novice AI/BI developers.

Learn practical data querying, transformation, and analysis using DuckDB's
in-process SQL engine -- no database server required.

## Prerequisites

- Python 3.10+
- [uv](https://docs.astral.sh/uv/) -- package manager and task runner
- [Task](https://taskfile.dev/) -- task runner (optional, for convenience commands)

## Quick start

```sh
uv sync                # install dependencies
task run               # lint/format, then run the main demo
```

Or without Task:

```sh
uv sync
uv run python -m duckdb_demo.main
```

## Project layout

```
src/duckdb_demo/
  __init__.py          # package metadata
  __main__.py          # python -m entry point
  main.py              # CLI runner -- walks through demo queries
  queries.py           # reusable SQL queries and helpers
  data/
    sales.csv          # sample dataset
examples/
  01_quick_start.py    # query a CSV with zero setup
Taskfile.yml           # task definitions (format, lint, run, test, ...)
pyproject.toml         # project config, ruff + ty settings
```

## Available tasks

Run `task` (or `task --list-all`) to see everything:

| Task | Description |
|---|---|
| `task run` | Auto-fix lint/format, then run the main demo |
| `task run:example -- <file>` | Run a standalone example script |
| `task fmt` | Auto-format with ruff |
| `task fmt:check` | Check formatting (no writes) |
| `task lint` | Lint + auto-fix with ruff |
| `task lint:check` | Lint only (no fixes) |
| `task typecheck` | Type-check with ty |
| `task check` | Run all quality checks (lint, format, type-check) |
| `task fix` | Auto-fix lint + reformat |
| `task test` | Run tests with pytest |
| `task sync` | Install/update dependencies |
| `task clean` | Remove caches and build artifacts |

## What the demo covers

- Loading CSV files directly into DuckDB (no pandas required)
- Aggregations (`GROUP BY`, `SUM`, `COUNT`)
- Date functions (`date_trunc`)
- Window functions (running totals)
- Parameterized top-N queries

## Toolchain

| Tool | Role |
|---|---|
| [uv](https://docs.astral.sh/uv/) | Package management, venv, task running |
| [DuckDB](https://duckdb.org/) | In-process SQL analytics engine |
| [ruff](https://docs.astral.sh/ruff/) | Linter + formatter |
| [ty](https://docs.astral.sh/ty/) | Type checker |
| [Task](https://taskfile.dev/) | Task runner |
