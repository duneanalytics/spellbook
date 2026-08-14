{# Turns the `meta.monitoring` blocks in the parsed manifest into rows for the per-subproject
   monitoring_config_spellbook_<subproject> writer models.

   Adapted copy of the canonical macros in duneanalytics/curated-data
   (macros/dune/monitoring_config.sql) — spellbook cannot import curated-data macros, so the
   duplication is deliberate. The row shape (table_name, event_time_column, partition_column,
   warn_seconds, critical_seconds, oncall) must stay identical to curated-data's: the
   `delta_prod.monitoring.config` view there unions every writer's table. Deviations from the
   canonical copy are spellbook-specific and commented where they occur. #}

{% macro _monitoring_seconds(after) %}
    {%- set units = {'minute': 60, 'hour': 3600, 'day': 86400} -%}
    {%- set period = after.get('period') if after is mapping else none -%}
    {%- set count = after.get('count') if after is mapping else none -%}
    {%- if units.get(period) is none or count is not number -%}{{ return(none) }}{%- endif -%}
    {{ return(count * units.get(period)) }}
{% endmacro %}

{# Built from config(schema=, alias=) rather than the resolved relation_name, which is
   target-dependent in catalog and in schema/alias — a CI compile would emit CI-schema names.
   This table declares what to monitor in production, so it emits prod-qualified names on every
   target.

   Catalog is hardcoded to `delta_prod` because that is what the lag metric carries
   (curated_table_lag_seconds{table="delta_prod.…"}, CUR2-3666). PromQL `on(table)` joins fail
   silently, so a `hive.`-prefixed name would leave every alert evaluating empty while still
   looking correctly configured.

   Deviation from the canonical copy: alias falls back to the node name — that is exactly what
   generate_alias_name does on prod when no alias is configured — and `no_schema` (the subproject
   default that scripts/check_schema.sh forbids on real models) counts as no schema. #}
{% macro _monitoring_relation(node) %}
    {%- set schema = node.config.get('schema') -%}
    {%- set alias = node.config.get('alias') or node.name -%}
    {%- if not schema or schema == 'no_schema' or not alias -%}{{ return(none) }}{%- endif -%}
    {{ return('delta_prod.' ~ schema ~ '.' ~ alias) }}
{% endmacro %}

{% macro _monitoring_event_time(node) %}
    {%- set event_time = node.config.get('event_time') -%}
    {%- if not event_time or event_time is not string -%}{{ return(none) }}{%- endif -%}
    {{ return(event_time) }}
{% endmacro %}

{# Not interchangeable with event_time: the CUR2-3673 scan fallback puts its WHERE here purely to
   prune partitions, and Trino's Delta connector will not derive a `block_month` filter from a
   `block_time` predicate.

   none when no partition column can be named — a legitimate state that still emits a row, unlike
   a missing event_time. The writer emits it as SQL NULL, which the scan generator reads as
   "prune on event_time_column".

   Deviation from the canonical copy: spellbook models declare `partition_by` (a list) instead of
   `properties.partitioned_by`, and position carries no convention here — dex.trades puts the time
   column first (['block_month', 'blockchain', 'project']), others put it last. So the time column
   is picked by name: the first column with a time-word token. No match falls back to NULL rather
   than guessing a position. #}
{% macro _monitoring_partition_column(node) %}
    {%- set partition_by = node.config.get('partition_by') -%}
    {%- if not partition_by -%}{{ return(none) }}{%- endif -%}
    {%- set cols = [partition_by] if partition_by is string else partition_by | list -%}
    {%- set time_tokens = ['time', 'timestamp', 'date', 'day', 'week', 'month', 'year', 'hour', 'minute'] -%}
    {%- for col in cols -%}
        {%- if col is string -%}
            {%- for token in col.lower().split('_') -%}
                {%- if token in time_tokens -%}{{ return(col) }}{%- endif -%}
            {%- endfor -%}
        {%- endif -%}
    {%- endfor -%}
    {{ return(none) }}
{% endmacro %}

{# A model whose declaration cannot produce a row is skipped, not raised on. This walks every model
   in the subproject, so raising would let one malformed block anywhere stop this table — and every
   alert reading it — from updating, over a model its owner never touched.

   Each subproject compiles only its own manifest (spellbook subprojects deploy independently), so
   each writer sees exactly its own subproject's declarations — that is why there is one writer
   model per subproject rather than one global one. #}
{% macro monitoring_config_rows() %}
    {#- graph is {} while parsing; the model falls back to its empty shape until compile. -#}
    {%- if not execute -%}{{ return([]) }}{%- endif -%}
    {%- set rows = [] -%}
    {%- for uid, node in graph.nodes.items() -%}
        {%- if node.resource_type == 'model' -%}
            {%- set mon = (node.config.get('meta', {}) or {}).get('monitoring', {}) -%}
            {%- if mon is mapping and mon.get('enabled') -%}
                {#- declared_in is not projected; it only makes the sort total when one table is
                    declared twice. -#}
                {%- set row = {
                    'table_name': _monitoring_relation(node),
                    'event_time_column': _monitoring_event_time(node),
                    'partition_column': _monitoring_partition_column(node),
                    'warn_seconds': _monitoring_seconds(mon.get('warn_after')),
                    'critical_seconds': _monitoring_seconds(mon.get('critical_after')),
                    'oncall': mon.get('oncall', false),
                    'declared_in': node.original_file_path,
                } -%}
                {#- partition_column is legitimately none; the rest are not. -#}
                {%- if row.table_name and row.event_time_column and row.warn_seconds and row.critical_seconds -%}
                    {%- do rows.append(row) -%}
                {%- endif -%}
            {%- endif -%}
        {%- endif -%}
    {%- endfor -%}
    {#- Stable sorts in reverse key order give (table_name, declared_in), so the emitted row order
        is independent of manifest order. -#}
    {{ return(rows | sort(attribute='declared_in') | sort(attribute='table_name')) }}
{% endmacro %}
