# Spellbook Agent Instructions

This file is the shared source of truth for AI agents working in Spellbook. Claude Code reads it through `CLAUDE.md`; opencode and Cursor read it natively.

## Project Overview

Spellbook is Dune Analytics' open-source interpretation layer for blockchain data. It is a dbt monorepo that transforms raw blockchain data into clean, usable datasets using SQL and Jinja on DuneSQL, which is Trino-based.

The repo is organized as independent dbt sub-projects under `dbt_subprojects/`:

- `daily_spellbook` - Default location for new spells, refreshed daily
- `hourly_spellbook` - Promoted higher-frequency spells, Dune-team managed
- `dex` - DEX and DEX aggregator data, including `dex.trades`
- `nft` - NFT-related models
- `solana` - Solana-specific models
- `tokens` - Token metadata, transfers, balances, and prices

Shared resources live at the repo root:

- `sources/` - Shared source YAML files for raw table definitions
- `dbt_macros/` - Shared macros such as `expose_spells`, `optimize_spell`, `mark_as_spell`, and `incremental_predicate`
- `scripts/` - Development utilities, including query runners and source generators
- `docs/` - Documentation for models, tests, seeds, macros, CI, and best practices
- `.claude/skills/` - Shared task-specific skills for Claude Code, opencode, and Cursor

## Documentation Map

Start with these docs before changing models:

- `docs/general/repo_navigation.md` - GitHub issues, PRs, discussions, and Actions
- `docs/general/best_practices.md` - Development workflow, performance, and incremental model guidance
- `docs/models/model_overview.md` - Model layout, naming, materialization, and schema YAML expectations
- `docs/models/model_config_block.md` - Required dbt model config patterns
- `docs/tests/test_overview.md` - Required model, seed, and generic test patterns
- `docs/ci_test/ci_test_overview.md` - How Spellbook PR CI runs and where to find test tables
- `docs/general/faq_and_common_issues.md` - Common contributor and CI problems

## Commands

Set up the locked Python environment from the repo root:

```bash
uv sync --locked
```

Run dbt commands through `uv run`. Commands that operate on a sub-project must target or run from that sub-project:

```bash
uv run dbt deps --project-dir dbt_subprojects/<subproject>/
uv run dbt compile --project-dir dbt_subprojects/<subproject>/
uv run dbt compile --select <model_name> --project-dir dbt_subprojects/<subproject>/
uv run dbt test --select <model_name> --project-dir dbt_subprojects/<subproject>/
```

To compile and run a model against Dune API, use the query helper from the repo root:

```bash
uv run python scripts/dune_query.py "@model_name" --limit 100
uv run python scripts/dune_query.py "SELECT * FROM dex.trades LIMIT 10"
uv run python scripts/dune_query.py --sql-file query.sql
```

`scripts/dune_query.py` requires `DUNE_API_KEY` in `.env` and auto-detects the sub-project for `@model_name` syntax.

## Development Workflow

1. Start with a working query in the Dune app.
2. Convert the query to a dbt model with the correct materialization.
3. Compile locally with dbt and inspect or test the compiled SQL on Dune.
4. Use short hardcoded date filters for initial development and CI iteration on large sources.
5. Submit a PR and use CI tables for QA.
6. Revert temporary hardcoded date filters before merge; production models should use normal incremental logic.

Always use `source()` and `ref()` for table references. Do not hardcode upstream table names in model SQL.

## Model Structure

Model paths depend on spell type:

- Dedicated sector sub-project: `dbt_subprojects/<project>/models/<alias>/<blockchain>/platforms/`
- Sector-level in daily/hourly: `dbt_subprojects/<project>/models/_sector/<sector>/<alias>/<blockchain>/`
- Project-level: `dbt_subprojects/<project>/models/_project/<project_name>/<blockchain>/`

Model files are named with the pattern `<project_version_blockchain_alias.sql>`, for example `uniswap_v3_arbitrum_base_trades.sql`. The file name is the dbt model name.

Every model SQL file must have a corresponding `_schema.yml` entry in the same directory.

## Model Config Rules

All models must explicitly declare:

- `schema` - Dune app schema name
- `alias` - Dune app table or view name
- `materialized` - `view`, `table`, or `incremental`

Additional materialization requirements:

- `table` requires `file_format='delta'`.
- `incremental` requires `file_format='delta'`, `incremental_strategy`, and `unique_key`.
- If a model is partitioned, include partition columns in `unique_key`.
- Use `incremental_predicate()` for `incremental_predicates`; do not hardcode equivalent predicates.
- Use `incremental_predicate()` only for time-series data. Omit it when full-history lookups are required, such as pool creation events.

Config-only changes that affect physical tables may not trigger CI because CI selects with `state:modified.body` and `state:modified.macros`. If the SQL body otherwise does not change, add or bump a body stamp comment for changes to `materialized`, `partition_by`, `incremental_strategy`, `file_format`, `schema`, or `alias`:

```sql
-- ci-stamp: 1
```

## Incremental Models

- Unique key columns must not contain NULL values. Trino NULL merge lookups fail silently and can create duplicates.
- Use `coalesce()` or `dbt_utils.generate_surrogate_key()` when key columns can be NULL.
- Keep unique key columns consistent across the model config, `_schema.yml` tests, and seed match columns.
- Use trailing Jinja whitespace control only, for example `{% if is_incremental() -%}`, `{% else -%}`, `{% endif -%}`.
- Apply incremental filters to all time-series sources in the query.
- The non-incremental path should filter from the earliest date of activity.

## DuneSQL Conventions

- Use `varbinary` for addresses and hashes. Hex literals should be unquoted, for example `0x039e...`, not `'0x039e...'`.
- Use `uint256` and `int256` for large numbers.
- `block_date` is `DATE`; `block_time` is `TIMESTAMP`.
- Prefer native bytearray and numeric functions over casts.
- Never use `select *` on large tables.
- Filter partition columns such as `block_date`, `block_time`, and `blockchain` where applicable.
- Put larger tables on the left side of joins when manually tuning performance.
- Include time filters in both `on` and `where` clauses when joining on partition columns.
- Use `union all` unless deduplication is required.
- Do not use `order by` without `limit` on large result sets.
- Only partition tables when each partition is expected to hold roughly 1M+ rows.

## Sector Spell Architecture

The standard sector lineage pattern is:

1. Platform base spells per project, version, and chain. These should contain raw data and are commonly incremental.
2. Chain-level union models that union platform spells on one chain. Materialize as tables to isolate chains.
3. Cross-chain union models that union chain-level spells. Materialize as views.
4. Final sector spells that enrich the cross-chain base with metadata.

Use macros for repeated logic across chains or forked protocols. Keep upstream base models simple and save metadata enrichment for downstream models.

## Schema YAML And Tests

Use dbt 1.10+ generic test syntax. For tests with inputs, put inputs under `arguments:` and configs under `config:`.

Required for materialized models:

- `dbt_utils.unique_combination_of_columns` on the exact unique key columns from the model config.
- `not_null` on each unique key column.
- Model descriptions and key column descriptions.

Use `data_tests:` for new or edited model-level tests. Do not use deprecated test argument syntax, and do not introduce misspelled keys such as `cdata_tests`.

Example:

```yaml
data_tests:
  - dbt_utils.unique_combination_of_columns:
      arguments:
        combination_of_columns:
          - block_date
          - tx_hash
          - evt_index
```

## Seeds

Seeds are required for sector-level spells and encouraged for standalone project spells.

- Register seeds in the directory `_schema.yml` with proper column data types.
- Include all unique key columns and the fields being tested.
- Keep seeds small; a handful of representative rows is enough.
- Seed unique key columns must exactly match the model `unique_key`.
- Seed column types must match the DuneSQL types produced by the model.
- CI runs `dbt seed` for modified seeds before comparing model output during `dbt test`.

## CI

PRs trigger per-sub-project GitHub Actions such as `daily_spellbook.yml`, `dex.yml`, `hourly_spellbook.yml`, `nft.yml`, `solana.yml`, and `tokens.yml`. These call the shared reusable workflow in `.github/workflows/dbt_run.yml`.

CI builds and tests only modified models where possible. Test tables are visible on Dune for about 24 hours. Current CI schemas use the pattern:

```sql
dune.dune_spellbook_ci__tmp_pr<PR>_<run_id>_<attempt>.<model_name>
```

Older CI runs may use legacy tables like:

```sql
dune.test_schema.git_dunesql_<GIT_HASH>_<schema>_<alias>
```

A green CI check is necessary but not sufficient. Query CI tables to verify data quality, especially for model logic changes.

## Skills

Use `.claude/skills/<name>/SKILL.md` for shared task-specific workflows. This path is read by Claude Code, opencode, and Cursor. Do not add shared instructions to `.cursor/rules`; promote always-on conventions to this file and procedural workflows to skills.
