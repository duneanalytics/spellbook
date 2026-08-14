{# Turns the `meta.monitoring` blocks in the parsed manifest into rows for the monitoring config
   writer models. CUR2-3670.

   CANONICAL PAIR — kept in sync by hand, since neither repo can import the other's macros:
       curated-data:  macros/dune/monitoring_config.sql
       spellbook:     dbt_macros/dune/monitoring_config.sql
   Same structure, same interfaces, same comments; only genuinely repo-specific behavior diverges,
   contained inside the macro it belongs to and marked with a "Repo-specific:" comment naming the
   counterpart's behavior. When editing one file, diff it against the other and mirror the change.
   Destined for the dune-dbt shared macro package (duneanalytics/dune-dbt draft PR #1) once that
   exists. #}

{% macro _monitoring_seconds(after) %}
	{%- set units = {'minute': 60, 'hour': 3600, 'day': 86400} -%}
	{%- set period = after.get('period') if after is mapping else none -%}
	{%- set count = after.get('count') if after is mapping else none -%}
	{%- if units.get(period) is none or count is not number -%}{{ return(none) }}{%- endif -%}
	{{ return(count * units.get(period)) }}
{% endmacro %}

{# Built from config(schema=, alias=) rather than the resolved relation_name, which is
   target-dependent in catalog and in schema/alias — a `--target ci` compile would emit CI-schema
   names. This table declares what to monitor in production, so it emits prod-qualified names on
   every target.

   Catalog is hardcoded to `delta_prod` because that is what the lag metric carries
   (curated_table_lag_seconds{table="delta_prod.…"}, CUR2-3666). PromQL `on(table)` joins fail
   silently, so a `hive.`-prefixed name would leave every alert evaluating empty while still
   looking correctly configured.

   alias falls back to the node name — what generate_alias_name resolves to in production in both
   repos when no alias is configured. curated-data mandates an explicit alias at parse time, so the
   fallback is unreachable there; spellbook models routinely rely on it. `no_schema` is spellbook's
   must-override placeholder default, never a real schema, so it counts as undeclared; no
   curated-data schema uses the name. #}
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
   `block_date` predicate.

   none when the model declares no partitioning — a legitimate state that still emits a row, unlike
   a missing event_time. The model emits it as SQL NULL, which the scan generator reads as "prune on
   event_time_column".

   Repo-specific: spellbook declares `partition_by` (a list) instead of `properties.partitioned_by`,
   and position carries no convention here — dex.trades puts the time column first
   (['block_month', 'blockchain', 'project']), others put it last — so the time column is picked by
   name: the first column with a time-word token, NULL when none matches rather than a positional
   guess. (Counterpart: curated-data parses its `properties.partitioned_by` Trino literal string
   and takes the last element by convention.) #}
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
   in the project, so raising would let one malformed block anywhere stop this table — and every
   alert reading it — from updating, over a model its owner never touched. Rejecting malformed
   declarations, naming the offending model, is each repo's own CI job (curated-data:
   scripts/validate_monitoring.py plus the mandatory schema/alias parse guards; spellbook: nothing
   dedicated yet, so a declaration that cannot parse silently drops from the table).

   Each dbt project compiles only its own manifest, so a writer sees exactly its own project's
   declarations. That is why spellbook — a monorepo of independently deployed dbt subprojects —
   carries one writer model per subproject rather than a global one. #}
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
	{#- Stable sorts in reverse key order give (table_name, declared_in), so the emitted row order is
	    independent of manifest order. -#}
	{{ return(rows | sort(attribute='declared_in') | sort(attribute='table_name')) }}
{% endmacro %}

{# The writer model's entire body: the declared rows as a VALUES relation, or a typed empty shape
   when nothing declares monitoring (also the parse-time shape, while graph.nodes is empty). Lives
   here rather than in the writer models so each writer stays a thin shim — config block, comment,
   one macro call — and the column set every arm must share is written in exactly one place per
   repo: the monitoring.config view in curated-data unions all the arms. #}
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
