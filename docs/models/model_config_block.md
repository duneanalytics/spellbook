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


## Default Table Properties

Each subproject defines `vars.dune_default_table_properties` in its `dbt_project.yml`.
The shared table-creation macro merges these defaults with the model's `properties`,
with model values taking precedence. This avoids dbt-trino's replacement of an entire
project-level `+properties` dictionary when a model supplies its own.

CDF defaults to `change_data_feed_enabled: 'true'`. Existing `partition_by` configs
continue to work and inherit this default. To opt out while retaining partitioning:

```sql
{{ config(
    schema='example',
    alias='events',
    materialized='table',
    file_format='delta',
    partition_by=['block_month'],
    properties={'change_data_feed_enabled': 'false'}
) }}
```

Property values are SQL expressions, for example `'true'` or `"ARRAY['block_month']"`.
Precedence is shared defaults, then `partition_by` translated to `partitioned_by`,
then explicit model `properties`. Generated S3 locations and CI visibility properties
are applied afterward by the existing macro.

These defaults and model properties apply when creating or rebuilding persistent
tables, including full refreshes. They do not apply to temporary tables or views,
and ordinary incremental runs do not alter existing tables. Enable CDF on an existing
table without rebuilding with
`ALTER TABLE catalog.schema.table_name SET PROPERTIES change_data_feed_enabled = true`.
CDF records changes after enablement; it does not backfill past changes.

## Unique Key Metadata

On prod runs, all Dune property post-hooks publish `dune.unique_key_columns` as a JSON list
inherited from the model's `unique_key`. A single string becomes a one-element list; composite
keys retain their column order. Each entry must be a column name, either bare or double-quoted
for reserved words: `unique_key = ['block_month', '"from"']` publishes `["block_month","from"]`.
Expressions fail compilation. Set `meta.dune.unique_key_columns` to override the published key,
including for full-table materializations without an incremental key:

```yaml
models:
  - name: my_full_table_model
    config:
      meta:
        dune:
          unique_key_columns: [blockchain, address]
```

The override must be a non-empty list of non-empty column names. It also works on incremental
models and does not change dbt's merge key. Models without either declaration omit the property;
full-table models should declare their row identity explicitly. The property is applied on every
prod run, including incremental runs and full refreshes. As with the other property post-hooks,
it is not applied on dev or CI targets.


## Freshness Monitoring

`meta.monitoring` declares when a production table should warn or page for stale data. The
monitoring config writer reads the resolved declaration from each subproject's dbt manifest.

There are no project defaults. dbt replaces the nested `monitoring` mapping rather than merging
individual fields, so every enabled declaration must include the complete block:

| Field | Type | Meaning |
|---|---|---|
| `enabled` | boolean | Whether the model is monitored. |
| `warn_after` | `{count, period}` | Staleness at which the model warns without paging. |
| `critical_after` | `{count, period}` | Staleness at which the model is critical. Must be greater than `warn_after`. |
| `oncall` | boolean | Whether a critical breach pages on-call. |

`count` must be a positive integer. `period` must be `minute`, `hour`, or `day`.

An enabled model must also set dbt's native `event_time` config to a documented event timestamp
column. Never use a write or load timestamp: it advances during rewrites and can appear fresh while
the source data is stale.

Declare monitoring in the model's schema YAML so threshold-only changes remain config-only and do
not rebuild the model:

```yaml
models:
  - name: dex_trades
    meta:
      monitoring:
        enabled: true
        warn_after: {count: 4, period: hour}
        critical_after: {count: 12, period: hour}
        oncall: true
    config:
      event_time: block_time
```

No `monitoring` block means the model has not been considered. `enabled: false` records an explicit
opt-out and requires no thresholds or `event_time`. CI validates declarations against each parsed
manifest with `scripts/validate_monitoring.py`.

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

4. **filtering_columns**
   - List of columns the Data Explorer suggests filtering on, ordered from most to least discriminating. Emitted as the `dune.data_explorer.filtering_columns` table property by the post-hook macros on prod runs.
   - Only set columns that actually reduce the data scanned: partition columns of the underlying tables, or a column that is constant per file (such as `blockchain` in a cross-chain union).
   - Partitioned tables do not need it — the catalog service defaults filtering columns to the partition columns when the property is absent. Set it for views and unpartitioned tables, which otherwise get no hint at all.
   - **Example**: `filtering_columns=['block_month', 'blockchain']` on a cross-chain view whose upstream tables are partitioned by `block_month`.
   - Set `filtering_columns=[]` to withdraw a hint that has already been published. Deleting the config line is not enough: view property updates only upsert the keys they send, so the last published value stays in place. On a table an empty list falls back to the partition-derived default.
