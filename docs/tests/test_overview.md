# Tests Tied to Models

Tests tied to models are highly encouraged and at times required, depending on the spell involved. If your spell contains complex logic or has heavy usage downstream on the Dune app, it's helpful to apply tests upfront during development and over time as bugs are identified.

## Required Tests

These tests are expected on all materialized models (incremental and table):

- **`dbt_utils.unique_combination_of_columns`** — applied to the unique key columns defined in the model config block. Ensures no duplicate rows exist. The columns tested must *exactly* match the `unique_key` in the config block.
- **`not_null`** — applied to each column in the unique key. NULLs in unique key columns cause merge lookups to fail silently in Trino, leading to duplicates.

These tests are defined in the model's `_schema.yml` file.

## Seed Tests (Sector-Level Spells)

For sector-level spells, seed tests are **required** to maintain data quality across contributions. The pattern:

1. Build a seed CSV with expected output rows (see [seed overview](../seeds/seed_overview.md))
2. Call the generic seed test in the model's `_schema.yml` (e.g., `check_dex_base_trades_seed`, `check_seed`)
3. CI runs the model, then compares output against the seed — any mismatch fails the test

The underlying comparison logic lives in the [`check_seed_macro`](/dbt_macros/generic-tests/check_seed_macro.sql).

For standalone project spells, seed tests are encouraged but not strictly required.

## Other Tests

Additional tests used across Spellbook:

- **Column type validation** — the [`check_column_types_macro`](/dbt_macros/generic-tests/check_column_types_macro.sql) can validate that model output columns match expected DuneSQL data types. Available but not widely used currently.
- **Custom project tests** — project-specific assertions (e.g., row count checks, value range validations). See existing examples throughout the repo.

## Where to Store Tests

Tests live inside each sub-project's `tests/` directory:

- **Generic tests** (reusable, called from `_schema.yml`): `dbt_subprojects/<project>/tests/generic/`
- **Project-specific tests**: `dbt_subprojects/<project>/tests/<project_name>/` or `dbt_subprojects/<project>/tests/_project/<project_name>/`

For more on how tests execute during CI, see [CI overview](../ci_test/ci_test_overview.md).
