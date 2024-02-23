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
   - `table` – Full refresh 1x/day, around 12pm EST.
   - `incremental` – Pull approximately the last day of data to load into the spell every 1 hour.
   - **Note**: There may be models without this property assigned, where a default value of ‘view’ is set in the dbt_project file. Please add this directly into the model moving forward.

## Required Configs for Incremental Tables

1. **file_format**

   - `delta` – Delta lake, an open-source storage framework, is used for all spells.

2. **incremental_strategy**

   - `merge` – Standard setting; some spells might use ‘append-only’ in unique situations.

3. **unique_key**

   - Primary key(s) that determine unique rows and specify join conditions in merge statements.

4. **incremental_predicates**
   - Filters the target to the same date range as the source, for improved performance & less data in memory.
   - Universal incremental macro can be found [here](/macros/incremental_predicate.sql).
   - **Note**: This is a newer addition to Spellbook. Please add this property for new incremental spells.

## Optional Configs for Materialized as Table / Incremental

1. **partition_by**
   - Useful for large tables (millions+ of rows). Partition by columns used in where clauses, group by's, or join conditions.
   - Examples: `block_month`, `blockchain`.
   - **Note**: Avoid partitioning by columns that are too granular and lead to too many partitions (e.g., `block_number` or `block_time`).

## Other Optional Properties for All Materialization Types

1. **post_hook**

   - In general, this can be any query needed to run after the model completes.
   - Main use in Spellbook: Add table properties for display on the Dune data explorer.
   - Ideal for spells at the end of a lineage, intended for frequent querying and public sharing.
   - Spells which are ‘building blocks’ towards final downstream spells can avoid this property.

2. **tags**

   - Tags are mostly used for Dune team to handle orchestration.
   - **Examples**: ‘prod_exclude’ for failing models or models not intended for production, ‘static’ for spells materialized as a table, yet only contain hardcoded static data and don’t need to run every day, only when modified.

3. **on_table_exists**
   - Overrides existing behavior for how a table is rebuilt on full refresh.
   - **Example**: `drop` to overcome dbt-trino bugs when changing a spell from view to table.
   - **Note**: This property is rare and usually applied by the Dune team.
