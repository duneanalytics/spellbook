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

{%- set rows = monitoring_config_rows() %}

{%- if rows | length == 0 %}

{#- No model declares monitoring (also the parse-time shape, when graph.nodes is empty). -#}
select
    cast(null as varchar) as table_name,
    cast(null as varchar) as event_time_column,
    cast(null as varchar) as partition_column,
    cast(null as integer) as warn_seconds,
    cast(null as integer) as critical_seconds,
    cast(null as boolean) as oncall
where false

{%- else %}

select
    table_name,
    event_time_column,
    partition_column,
    warn_seconds,
    critical_seconds,
    oncall
from (
    values
    {%- for r in rows %}
        ('{{ r.table_name }}', '{{ r.event_time_column }}', {{ "'" ~ r.partition_column ~ "'" if r.partition_column else 'cast(null as varchar)' }}, {{ r.warn_seconds }}, {{ r.critical_seconds }}, {{ 'true' if r.oncall else 'false' }}){{ ',' if not loop.last }}
    {%- endfor %}
) as t (table_name, event_time_column, partition_column, warn_seconds, critical_seconds, oncall)

{%- endif %}
