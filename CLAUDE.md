# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**Apollo** — a Salesforce CSV-to-Oracle ETL pipeline. Reads CSV exports, infers Oracle types, generates/alters DDL, and bulk-loads via `executemany` with named binds.

All working source lives under `.bin/`. The repo root (`Q:/olympus`) is just the workspace container.

## Commands

All commands must be run from `.bin/`:

```bash
cd .bin

# Run all tests
pytest

# Run a single sprint test file
pytest tests/test_sprint1_foundation.py

# Run a specific test by name
pytest tests/test_sprint1_foundation.py::test_column_map_defaults

# Run with verbose output
pytest -v
```

The `pyproject.toml` sets `pythonpath = ["."]` so imports resolve from `.bin/` as root. The `.venv` is at the repo root (`Q:/olympus/.venv`).

## Architecture

The pipeline is a six-phase ETL process orchestrated by `src/pipeline.py`:

### Phase Flow

| Phase | File(s) | Responsibility |
|---|---|---|
| 1 — Models | `src/models/models.py` | `ColumnMap` + `TableMeta` dataclasses; cached named-bind INSERT SQL |
| 2 — Local Sniff | `src/discovery/local_sniff.py` | Full CSV scan: alignment, size tracking, early breach detection, type inference |
| 3+4 — Oracle Discovery | `src/discovery/remote_discovery.py`, `ddl_builder.py`, `oracle_client.py` | Query `ALL_TAB_COLUMNS`; generate CREATE/ALTER DDL; resize columns |
| 5 — Row Generator | `src/transformers/row_generator.py`, `normalizers.py` | Lazy stream of `dict[oracle_name, value]` cleaned for Oracle insert |
| 6 — Batch Load | `src/loaders/batch_exec.py`, `binds.py` | `executemany` with `batcherrors=True`; commit; log anomalies |

**Entry point:** `src/pipeline.py` exposes `run()` (full pipeline) and `validate()` (Phases 1–2 only, no DB).

### Key Design Decisions

- **Named binds only** — all DML uses `:oracle_name` syntax. No positional binds anywhere.
- **`insert_sql` is cached** on `TableMeta` after Phase 4 metadata refresh. Call `invalidate_sql_cache()` before modifying `columns` post-cache.
- **Sanitizer is the SQL injection boundary** — `src/utils/sanitizer.py` is the only place raw CSV headers become SQL identifiers. Raw strings never reach a query string directly.
- **Quarantine policy** — `QuarantineError` (alignment or size breach) at Phase 2, or `DDLError` at Phase 3, moves the file to `error_dir` and halts. Load errors at Phase 6 are logged but do not quarantine (partial commit may have occurred).
- **VARCHAR2 hard cap** — 4000 CHAR. No CLOB fallback. Size breaches caught at Phase 2 (primary) and Phase 4 (secondary safety net).
- **CHAR semantics always** — `VARCHAR2(N CHAR)`, never byte semantics.
- **Type inference** — NUMBER, DATE, TIMESTAMP where unambiguous; fallback to VARCHAR2 on any ambiguity without raising.

### Package Map

```
src/
  pipeline.py          # Orchestrator — the single callable for a full run
  configs/
    config.py          # PipelineConfig dataclass; env var overrides for all constants
    exceptions.py      # IngestionError → QuarantineError (AlignmentError, SizeBreachError), DDLError
    csv_dialect.py     # Strict CSV dialect with BOM handling
  models/
    models.py          # ColumnMap, TableMeta
  discovery/
    base.py            # AbstractSource interface
    csv_reader.py      # SF CSV reader (utf-8-sig, strict dialect)
    sf_reader.py       # Salesforce-specific CSV reader
    local_sniff.py     # Phase 2: full file scan → populated TableMeta
    oracle_client.py   # Oracle connection management
    remote_discovery.py# Phase 3: ALL_TAB_COLUMNS queries, Scenario A/B detection
    ddl_builder.py     # CREATE TABLE / ALTER TABLE ADD / MODIFY generators
  transformers/
    typing_infer.py    # Oracle type inference from CSV cell values
    normalizers.py     # \x00 strip, empty→None, date parse, Decimal conversion
    row_generator.py   # Phase 5: lazy generator of named-bind dicts
  loaders/
    binds.py           # Named bind type mapping; setinputsizes dict builder
    batch_exec.py      # Phase 6: executemany wrapper, batcherrors=True
    error_logging.py   # Anomaly log sink
  utils/
    sanitizer.py       # Identifier regex, reserved word (_COL suffix), truncation
    identifiers.py     # to_table_name(), to_schema_name(), to_column_name() wrappers
    validation.py      # Header alignment, row field count checks
    files.py           # quarantine_file(), mark_processed()
```

### Configuration

`PipelineConfig` (dataclass, `src/configs/config.py`) holds all tuneable constants. All fields read from environment variables with sensible defaults:

- `VARCHAR2_GROWTH_BUFFER` (default 50) — buffer added to observed max char length when sizing columns
- `BATCH_SIZE` (default 1000) — rows per `executemany` call
- `ORACLE_MAX_IDENTIFIER_LEN` (default 30 for legacy Oracle, 128 for >=12.2)
- `INCOMING_DIR`, `PROCESSED_DIR`, `ERROR_DIR` — file movement paths
- `DRY_RUN` — generates DDL/SQL but makes no DB calls and moves no files

### Sprint Test Structure

Tests in `tests/` are cumulative sprint tests — each re-affirms all prior sprint contracts plus its own. A passing sprint test means the chain is intact end-to-end up to that sprint.

Sprint progression: Foundation (1) → CSV Sniff (2) → Type Inference (3) → Oracle DDL (4) → Transform (5) → Load (6) → Pipeline (7).

Sprint design documents and reference implementations live in `docs/sprints/Sprint N/` and are not production code.
