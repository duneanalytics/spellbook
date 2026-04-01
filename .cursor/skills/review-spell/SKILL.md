# Review Spell PR

Use this skill when reviewing a Spellbook spell contribution (new model or modification). Walk through each section below as a checklist.

## 1. Config Block

- [ ] `schema` is present and correct for the Dune namespace
- [ ] `alias` is present and matches intended table/view name
- [ ] `materialized` is explicitly declared (not relying on dbt_project.yml default)
- [ ] If `table` or `incremental`: `file_format='delta'` is present
- [ ] If `incremental`:
  - [ ] `incremental_strategy` specified (`merge`, `append`, or `delete+insert`)
  - [ ] `unique_key` specified — no columns that could contain NULLs
  - [ ] `incremental_predicates` uses the `incremental_predicate()` macro (unless full-history lookup needed)
  - [ ] If partitioned: partition column(s) included in `unique_key`
- [ ] Config block formatting follows `sql-style-guide.mdc` (tabs, trailing comma on last param, `{{ config(` on first line)

## 2. Schema YML (`_schema.yml`)

- [ ] Model entry exists with name matching the SQL filename
- [ ] Model has a description
- [ ] `dbt_utils.unique_combination_of_columns` test present — columns match config `unique_key` exactly
- [ ] `not_null` test on each unique key column
- [ ] Key columns have descriptions
- [ ] For sector-level spells: seed test is present (e.g., `check_dex_base_trades_seed`)

## 3. SQL Style (per `sql-style-guide.mdc`)

- [ ] Leading commas (left comma club)
- [ ] Tab indentation (no spaces)
- [ ] All SQL keywords and function names lowercase
- [ ] New line after `select`, `from`, `where`, `group by`, `order by`, etc.
- [ ] Explicit join types (`inner join`, `left join` — never bare `join`)
- [ ] Table aliases use `as` keyword (`from users as u`, not `from users u`)
- [ ] All columns prefixed with table aliases when joins are present
- [ ] CTEs use leading commas between them, `with` and CTE name on same line

## 4. Jinja

- [ ] All table references use `source()` or `ref()` — no hardcoded table names
- [ ] Jinja whitespace: trailing `-` only (`{% if -%}`, `{% else -%}`, `{% endif -%}`)
- [ ] `{% if is_incremental() -%}` block present for incremental models with:
  - Incremental path using `{{ incremental_predicate('source.block_time') }}`
  - Non-incremental path with earliest date filter

## 5. Performance

- [ ] Join order: larger table on left side
- [ ] Partition filters present in WHERE clauses (`block_date`, `block_time`)
- [ ] Cross-chain tables filtered by both `blockchain` and time
- [ ] No `SELECT *` on large tables
- [ ] `UNION ALL` used (not bare `UNION`) unless deduplication truly needed
- [ ] No `ORDER BY` without `LIMIT` on large result sets
- [ ] Time filters in both ON and WHERE clauses when joining on partition columns

## 6. Seed File (Sector-Level Spells)

- [ ] Seed CSV present with representative rows
- [ ] Seed registered in directory's `_schema.yml` with column types
- [ ] Seed columns include all unique key columns + fields to test
- [ ] Seed is small (handful of rows, not hundreds)
- [ ] Seed test called in model's `_schema.yml` with correct parameters

## 7. Architecture (Sector-Level Spells)

- [ ] Follows lineage: platform base → chain-level union (table) → cross-chain union (view) → final enriched spell
- [ ] Platform base spells use macros for forked protocols where applicable
- [ ] One model per protocol, per version, per blockchain
- [ ] Metadata enrichment saved for downstream (base spells contain raw data only)

## 8. Compile & Test

Run `dbt compile` in the relevant sub-project to verify:
- [ ] No compilation errors
- [ ] Compiled SQL in `target/` looks correct
- [ ] Test with `python scripts/dune_query.py "@model_name" --limit 100` or paste into Dune
