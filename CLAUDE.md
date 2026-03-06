# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Spellbook is Dune Analytics' open-source interpretation layer for blockchain data. It's a **dbt monorepo** that transforms raw blockchain data into clean, usable datasets ("spells") using SQL + Jinja2 templating on DuneSQL (Trino-based).

## Repository Structure

The repo is organized as multiple independent dbt sub-projects under `dbt_subprojects/`:

- **`daily_spellbook`** — Default location for new spells (daily refresh)
- **`hourly_spellbook`** — Promoted spells with higher frequency (requires Dune team approval)
- **`dex`** — DEX and DEX aggregator trading data (includes `dex.trades`)
- **`nft`** — NFT-related models
- **`solana`** — Solana-specific models
- **`tokens`** — Token metadata, transfers, and balances

Each sub-project is self-contained with its own `dbt_project.yml`, `profiles.yml`, `models/`, `macros/`, `seeds/`, and `tests/`.

Shared resources live at the repo root:
- `sources/` — 170+ source YAML files (raw table definitions)
- `dbt_macros/` — Shared macros (`expose_spells`, `optimize_spell`, `mark_as_spell`, `enforce_join_distribution`, `incremental_predicate`, etc.)
- `scripts/` — Development utilities (query runner, source generators)

## Commands

### Setup
```bash
pipenv install          # Create virtual environment (Python 3.9+)
pipenv shell            # Activate environment
```

### Build & Compile (must be run from a sub-project directory)
```bash
cd dbt_subprojects/<subproject>/
dbt clean               # Clean old artifacts
dbt deps                # Pull dbt dependencies
dbt compile             # Compile Jinja/SQL to plain SQL in target/
```

### Testing
```bash
# Run dbt tests for a model
dbt test --select @model_name

# Compile and run a dbt model against Dune API
python scripts/dune_query.py "@model_name" --limit 100

# Run raw SQL against Dune
python scripts/dune_query.py "SELECT * FROM dex.trades LIMIT 10"

# Run SQL from file
python scripts/dune_query.py --sql-file query.sql
```

The `dune_query.py` script requires a `DUNE_API_KEY` in `.env`. It auto-detects the correct sub-project directory for `@model_name` syntax.

### Pre-push Hooks (optional)
```bash
pre-commit install --hook-type pre-push
pre-commit run --hook-stage manual    # Manual run
```

### CI
PRs trigger GitHub Actions that run `dbt slim ci` — only modified models are tested. Test results appear in tables named `test_schema.git_dunesql_<commit_hash>_<table_name>` (available ~24 hours).

## DuneSQL / Trino SQL Rules

- **Always use explicit table aliases** — prefix all columns (`t.column`, `p.column`), never bare column names
- **Data types**: `block_date` is DATE (`DATE '2025-10-08'`), `block_time` is TIMESTAMP. Use `UINT256`/`INT256` for large numbers. Addresses are VARBINARY with `0x` prefix (not strings)
- **Partition filtering**: Filter on `block_date` (not `block_time`) for partition pruning. Always include partition columns in WHERE and JOIN conditions
- **Partitioning strategy**: `block_month` for large tables (trades, transfers), `block_date` for smaller tables
- **Performance**: Use `LIMIT` during development. Avoid `ORDER BY` on large result sets. Use `{{ enforce_join_distribution("PARTITIONED") }}` for large table joins. Larger table goes on left side of joins
- **Use `UNION ALL`** instead of `UNION` unless deduplication is needed

## dbt Model Patterns

### Materialization
Models default to `view`. Switch to `table` or `incremental` when performance requires it.

### Incremental Models
```sql
{{ config(
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    unique_key=['blockchain', 'tx_hash', 'evt_index', 'block_month'],
    incremental_predicates=[incremental_predicate('DBT_INTERNAL_DEST.block_time')]
)}}
```
- Include the partition column in `unique_key`
- Use `block_date` filters (not `block_time`) in non-incremental mode

### References
- `{{ ref('model_name') }}` — reference another dbt model (filename without .sql)
- `{{ source('schema', 'table') }}` — reference raw/decoded tables
- Never hardcode table names; always use refs/sources

### Exposing Models
Use the `expose_spells` macro in post_hook to make models publicly accessible:
```sql
{{ config(
    post_hook='{{ expose_spells(\'["ethereum"]\', "sector", "dex", \'["contributor"]\') }}'
)}}
```

### File Conventions
- One table/view/macro per file
- File name = model name
- Models must have a corresponding entry in `schema.yml` with description and tests
- Add `unique` and `not_null` tests to primary keys
- Directory convention: `models/<sector_or_project>/<chain>/`

## Key Macros (in dbt_macros/)

| Macro | Purpose |
|-------|---------|
| `expose_spells` | Makes model publicly accessible on dune.com |
| `optimize_spell` | Post-hook for table optimization (vacuum/analyze) |
| `mark_as_spell` | Tags model with metadata |
| `enforce_join_distribution` | Sets PARTITIONED distribution hint for joins |
| `incremental_predicate` | Generates partition-aware incremental predicates |
| `set_trino_session_property` | Sets Trino session properties |

## Testing Workflow

1. Write/modify model SQL
2. `dbt compile` in the sub-project to validate syntax
3. Copy compiled SQL from `target/` and test on dune.com, or use `python scripts/dune_query.py "@model_name"`
4. Add/update schema.yml with tests and descriptions
5. Submit PR — CI will create test tables for validation
6. Compare test vs prod: `test_schema.git_dunesql_<hash>_<table>` vs production table
