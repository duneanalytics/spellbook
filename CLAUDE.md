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
- `docs/` — Internal documentation on models, tests, seeds, macros, CI, best practices

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
PRs trigger GitHub Actions that run `dbt slim ci` — only modified models are tested. Each sub-project has its own workflow file (e.g., `dex.yml`, `tokens.yml`), all calling the shared reusable workflow in `dbt_run.yml`. Test results appear in tables named `test_schema.git_dunesql_<commit_hash>_<table_name>` (available ~24 hours).
