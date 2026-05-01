# Models

Models are simply SQL queries that transform raw data into a structured format suitable for analytics downstream on the Dune app. Think of models as the blueprint for how raw data is transformed into meaningful insights in easy-to-use end tables, i.e., spells.

## Naming Standards

### Directories

Models live inside sub-project directories. The path depends on whether the spell is sector-level or project-level:

- **Sector-level** (in dedicated sub-projects like `dex`, `nft`, `tokens`):
  `dbt_subprojects/<project>/models/<table_alias>/<blockchain>/platforms/`

  - **Example**: `dbt_subprojects/dex/models/trades/arbitrum/platforms/`

- **Sector-level** (in `daily_spellbook` or `hourly_spellbook`):
  `dbt_subprojects/<project>/models/_sector/<sector>/<table_alias>/<blockchain>/`

  - **Example**: `dbt_subprojects/hourly_spellbook/models/_sector/gas/fees/arbitrum/`
  - **Note**: The `_sector` prefix is used in `daily_spellbook` and `hourly_spellbook` to force sector directories to the top.

- **Project-level**:
  `dbt_subprojects/<project>/models/_project/<project_name>/<blockchain>/`

  - **Example**: `dbt_subprojects/daily_spellbook/models/_project/aave/ethereum/`

### File Names

- `<project_version_blockchain_alias.sql>`

  - **Example**: `uniswap_v3_arbitrum_base_trades.sql`

  - **Note**: Not all files will contain a version, or a specific blockchain, especially when at cross-chain level.

## Source & Schema YML Files

### Source YML Files

All sources are centralized in the [`sources/`](/sources) directory at the root of Spellbook, so they can be reused across sub-projects.

Required for dbt to understand the source schema & table names. Most source files include:

- Schema name of source
  - Typically base or decoded tables, but can be anything live on Dune.
- Table name of source
- (optional) Column names
- (optional) Tests / descriptions on columns

### Schema YML Files

Every model must have a corresponding entry in a `_schema.yml` file in the same directory. Schema files are required for dbt to understand the models within the project. Key areas:

- **Model name** — must match the SQL file name (without `.sql`)
- **Model description** — what the model produces and its purpose
- **Column descriptions** — at minimum for key columns
- **Tests** — critical for data quality:
  - `dbt_utils.unique_combination_of_columns` on the unique key columns (must match the config block's `unique_key`)
  - `not_null` tests on primary key columns
  - Seed tests (`check_dex_base_trades_seed`, `check_seed`, etc.) to validate model output against hardcoded expected results
- **Model tags** — helpful for `dbt ls` filters and project search

## Model Materialization

For full config block details, see [Model Config Block](./model_config_block.md).

- **View**

  - Spellbook default if not overwritten in the model config block. Views do not physically store data — the SQL query runs fully on each execution in the Dune app.
  - Views are the simplest to build and should be the starting point for each new standalone spell. The main reason to move away from a view is query performance downstream.

- **Incremental**

  - Best for spells with large, time-series datasets that need frequent updates. Incremental models only process new/changed data each run — the frequency depends on the sub-project (e.g., hourly in `hourly_spellbook`, daily in `daily_spellbook`).
  - Outside of the config block requirements, the model body requires:
    - `{% if is_incremental() -%}` checks (using trailing `-` only for whitespace control):
      - Apply on all sources with time-series data (events, transactions, etc.)
      - **Non-incremental path**: Full refresh / initial historical load. Apply a filter for the earliest date of activity for the model
      - **Incremental path**: Apply the [`incremental_predicate()`](/dbt_macros/dune/incremental_predicate.sql) macro on the source to match the predicate filter on the target in the config block. [Example usage](/dbt_subprojects/dex/macros/models/_project/uniswap_compatible_trades.sql#L29-L32)
  - Optional use cases:
    - Self-referencing the model using the `{{ this }}` syntax

- **Table**

  - Best for spells where data isn't frequently updated at the source, or where a full refresh is simpler than incremental logic. Tables perform a full refresh every run — the frequency depends on the sub-project.
  - Like incremental models, tables require `file_format='delta'` in the config block.
  - Common use cases: dimension tables, hardcoded static data, aggregation tables where incremental logic adds unnecessary complexity.
