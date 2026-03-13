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
    sales.csv          # sample sales dataset (CSV)
    inventory.xlsx     # sample inventory dataset (Excel)
examples/
  11_quick_start.py    # query a CSV with zero setup
  12_excel_to_duckdb.py # convert an Excel file to a persistent DuckDB database
Taskfile.yml           # task definitions (format, lint, run, test, ...)
pyproject.toml         # project config, ruff + ty settings
```

## Available tasks

Run `task` (or `task --list-all`) to see everything:

| Task | Description |
|---|---|
| `task run` | Auto-fix lint/format, then run the main demo |
| `task run:example -- <file>` | Run a standalone example script |
| `task run:track1` | Run all Track 1 — Foundations examples |
| `task run:track2` | Run all Track 2 — Core SQL examples |
| `task run:track3` | Run all Track 3 — Intermediate SQL examples |
| `task run:track4` | Run all Track 4 — Data I/O & Export examples |
| `task run:track5` | Run all Track 5 — Python Interop examples |
| `task run:track6` | Run all Track 6 — Advanced SQL examples |
| `task run:track7` | Run all Track 7 — Visualization examples |
| `task run:track8` | Run all Track 8 — Real-World Patterns examples |
| `task run:all-tracks` | Run all example tracks (1-8) sequentially |
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
- Reading Excel files with DuckDB's `excel` extension (`read_xlsx`)
- Converting Excel data to a persistent `.duckdb` database
- Aggregations (`GROUP BY`, `SUM`, `COUNT`)
- Date functions (`date_trunc`)
- Window functions (running totals)
- Parameterized top-N queries

## Toolchain

| Tool | Role |
|---|---|
| [uv](https://docs.astral.sh/uv/) | Package management, venv, task running |
| [DuckDB](https://duckdb.org/) | In-process SQL analytics engine |
| [openpyxl](https://openpyxl.readthedocs.io/) | Excel file support |
| [ruff](https://docs.astral.sh/ruff/) | Linter + formatter |
| [ty](https://docs.astral.sh/ty/) | Type checker |
| [Task](https://taskfile.dev/) | Task runner |
