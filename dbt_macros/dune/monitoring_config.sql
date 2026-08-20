{# Extracts `meta.monitoring` model declarations into the writer models' rows (CUR2-3670).
   Canonical pair with curated-data macros/dune/monitoring_config.sql (same macros and signatures,
   richer comments there) — keep the two in sync by hand; destined for the dune-dbt shared macro
   package (dune-dbt draft PR #1). #}

{% macro _monitoring_seconds(after) %}
	{%- set units = {'minute': 60, 'hour': 3600, 'day': 86400} -%}
	{%- set period = after.get('period') if after is mapping else none -%}
	{%- set count = after.get('count') if after is mapping else none -%}
	{%- if units.get(period) is none or count is not number -%}{{ return(none) }}{%- endif -%}
	{{ return(count * units.get(period)) }}
{% endmacro %}

{# delta_prod is hardcoded on purpose: table_name must match the lag metric's `table` label, and a
   PromQL on(table) join against any other spelling fails silently. #}
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

{# Repo-specific: the time column is picked from the `partition_by` list by name-token, position
   carrying no convention here. (curated-data: last element of `properties.partitioned_by`.) #}
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

{% macro monitoring_config_rows() %}
	{%- if not execute -%}{{ return([]) }}{%- endif -%}
	{%- set rows = [] -%}
	{%- for uid, node in graph.nodes.items() -%}
		{%- if node.resource_type == 'model' -%}
			{%- set mon = (node.config.get('meta', {}) or {}).get('monitoring', {}) -%}
			{%- if mon is mapping and mon.get('enabled') -%}
				{%- set row = {
					'table_name': _monitoring_relation(node),
					'event_time_column': _monitoring_event_time(node),
					'partition_column': _monitoring_partition_column(node),
					'warn_seconds': _monitoring_seconds(mon.get('warn_after')),
					'critical_seconds': _monitoring_seconds(mon.get('critical_after')),
					'oncall': mon.get('oncall', false),
					'declared_in': node.original_file_path,
				} -%}
				{%- if row.table_name and row.event_time_column and row.warn_seconds and row.critical_seconds -%}
					{%- do rows.append(row) -%}
				{%- endif -%}
			{%- endif -%}
		{%- endif -%}
	{%- endfor -%}
	{{ return(rows | sort(attribute='declared_in') | sort(attribute='table_name')) }}
{% endmacro %}

{% macro monitoring_config_select() -%}
{%- set rows = monitoring_config_rows() -%}
{%- if rows | length == 0 -%}
select
    cast(null as varchar) as table_name,
    cast(null as varchar) as event_time_column,
    cast(null as varchar) as partition_column,
    cast(null as integer) as warn_seconds,
    cast(null as integer) as critical_seconds,
    cast(null as boolean) as oncall
where false
{%- else -%}
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
{%- endif -%}
{%- endmacro %}
