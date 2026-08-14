{{ config(
    schema = 'monitoring'
    , alias = 'config_spellbook_dex'
    , materialized = 'table'
    , file_format = 'delta'
) }}

/*
    The dex subproject's arm of the freshness monitoring config: one row per model in this
    subproject declaring `meta.monitoring.enabled`, read out of the parsed manifest at compile
    time. The row shape mirrors duneanalytics/curated-data's monitoring_config_curated_data —
    the `delta_prod.monitoring.config` view in that repo unions every arm and resolves a table
    declared twice by priority (curated-data > sqlmesh > spellbook > datashare).

    One writer per subproject, `monitoring.config_spellbook_<subproject>`, because spellbook
    subprojects compile and deploy independently — a writer here can only see this subproject's
    manifest, and Delta takes no concurrent writers, so the subprojects cannot share one table.

    `table_name` is the prod-qualified physical relation consumers join on, not the model name —
    model.dex.dex_trades materializes as delta_prod.dex.trades. Emitted prod-qualified on every
    target, CI included: this table declares what to monitor in production.

    `partition_column` is not interchangeable with `event_time_column`: the CUR2-3673 scan
    fallback puts its WHERE on the partition column purely to prune partitions. NULL means
    "prune on `event_time_column`".

    No ref() on purpose: it reads declarations, not data, so it cannot fail because an upstream
    model failed, and it still emits a row for a table that was declared but never built.
*/

{{ monitoring_config_select() }}
