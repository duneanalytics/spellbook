# Model Config

Each model within Spellbook contains a config block with various properties. Depending on the type of model, different configurations are required.

## Required Config Properties

1. **schema**

   - Schema name, as used on the Dune app.
   - **Note**: There may be some older models with the schema property in the dbt project file, but this will eventually be moved directly into models. All future spells will require this property in the model.

2. **alias**

   - Table/view name, as used on the Dune app.

3. **materialized**
   - `view` – Consider performance downstream, as the view executes the underlying query each execution.
   - `table` – Full refresh every run (frequency depends on sub-project). Requires `file_format='delta'`.
   - `incremental` – Only adds/updates recent rows each run (frequency depends on sub-project). Requires `file_format`, `incremental_strategy`, and `unique_key`.
   - **Note**: There may be models without this property assigned, where a default value of 'view' is set in the dbt_project file. Please add this directly into the model moving forward.

## Required Configs for Incremental Tables

1. **file_format**

   - `delta` – Delta lake, an open-source storage framework, is used for all materialized spells (both `table` and `incremental`).

2. **incremental_strategy**

   - `merge` – Standard setting for most spells.
   - `append` – For append-only use cases where deduplication is not needed.
   - `delete+insert` – Rare; deletes matching rows in the target before inserting new ones.

3. **unique_key**

   - Primary key(s) that determine unique rows and specify join conditions in merge statements.
   - **Critical**: There cannot be NULLs in unique key columns — in Trino, NULLs cause merge lookups to fail silently, leading to duplicates. Use `coalesce()` on key columns, or `dbt_utils.generate_surrogate_key()` if columns may contain NULLs.
   - **Important**: If a table is partitioned, always include the partition column(s) in `unique_key` — this enables Trino to prune partitions during merge lookups, dramatically improving performance.

4. **incremental_predicates**
   - Filters the target table to the same date range as the source, for improved performance & less data in memory.
   - Always use the `incremental_predicate()` macro rather than hardcoding:
     ```sql
     incremental_predicates=[incremental_predicate('DBT_INTERNAL_DEST.block_time')],
     ```
   - Macro source: [`incremental_predicate.sql`](/dbt_macros/dune/incremental_predicate.sql).
   - Only use for time-series data — do NOT use when you need to check against full history (e.g., pool creation events).
   - **Note**: This is a newer addition to Spellbook. Please add this property for new incremental spells.

## Optional Configs for Materialized as Table / Incremental

1. **partition_by**
   - Useful for large tables (millions+ of rows). Partition by columns used in where clauses, group by's, or join conditions.
   - Common patterns:
     - `partition_by=['block_month']` — most common (trades, transfers, swaps)
     - `partition_by=['block_date']` — very high-volume tables
     - `partition_by=['blockchain', 'project', 'block_month']` — cross-chain sector spells
   - **Note**: Partitioning is NOT always beneficial — only use it for large tables where each partition contains 1M+ rows. Avoid partitioning by columns that are too granular (e.g., `block_number` or `block_time`).

## Other Optional Properties for All Materialization Types

1. **post_hook**

   - In general, this can be any query needed to run after the model completes.
   - Main use in Spellbook: Add table properties for display on the Dune data explorer.
   - Ideal for spells at the end of a lineage, intended for frequent querying and public sharing.
   - Spells which are 'building blocks' towards final downstream spells can avoid this property.

2. **tags**

   - Tags are mostly used for Dune team to handle orchestration.
   - **Examples**: 'prod_exclude' for failing models or models not intended for production, 'static' for spells materialized as a table, yet only contain hardcoded static data and don't need to run every day, only when modified.

3. **on_table_exists**
   - Overrides existing behavior for how a table is rebuilt on full refresh.
   - **Example**: `drop` to overcome dbt-trino bugs when changing a spell from view to table.
   - **Note**: This property is rare and usually applied by the Dune team.
