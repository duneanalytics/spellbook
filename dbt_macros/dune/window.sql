{#
  The incremental time window: incremental_filter(column) renders the body read, window_start()
  and window_end() render the bounds as SQL, all on one contract. incremental_predicate in
  incremental_predicate.sql is a deprecated wrapper over this file. Ported verbatim from
  curated-data (macros/dune/window.sql) so both projects share one window contract. Bounds
  are TIMESTAMP literals or SQL expressions, half open [start, end), UTC. Trino rewrites a
  TIMESTAMP literal against a date column, so partition pruning on `day` / `block_date` holds.

  Resolution order:
    1 batch       model.batch of a microbatch run: [event_time_start, event_time_end).
    2 run         DBT_EVENT_TIME_START / DBT_EVENT_TIME_END in the environment, both or none,
                  'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM:SS': the same pair dbt slices microbatches
                  from. A backfill, a restatement, a CI build and a local first build set it.
    3 first build any non-incremental build (a table or view rebuild, --full-refresh, a new
                  table) on prod reads `begin` config, else DBT_DEFAULT_BEGIN; on any other
                  target it is a compile error (CI exports a fixed pair).
    4 rolling     start = date_trunc(unit, now() - interval N unit), evaluated by Trino per
                  statement; end = now() (window_end only: incremental_filter leaves the upper
                  side open on a rolling run). N and unit: the lookback argument, else the model's
                  `meta.lookback_window` config ('18 hour'), else DBT_ENV_INCREMENTAL_TIME / _UNIT.

  The lookback lives under `meta` because dbt deprecates custom top-level config keys. In
  incremental_predicates config, dbt renders the call at parse time where config.meta_get() is
  empty, so a custom lookback is passed as the argument there:
      incremental_predicates = ["DBT_INTERNAL_DEST.day >= " ~ window_start(lookback='18 hour')]
  The rolling start is SQL on now() so such a predicate stays valid under the partial-parse
  cache; a pair literal in config is re-rendered because dbt re-parses files whose env_var()
  value changed. Setting DBT_RUN_START_DATE / DBT_RUN_END_DATE (env or --vars) is a compile
  error here. Shapes are pinned by dbt_subprojects/*/tests/window_shapes.sql.
#}

{% macro _window_error(msg) %}
	{{ exceptions.raise_compiler_error('window: ' ~ msg) }}
{% endmacro %}

{% macro _window_ts_literal(d) %}
	{#- naive or tz-aware datetime -> TIMESTAMP 'YYYY-MM-DD HH:MM:SS.fff' in UTC -#}
	{%- if d.tzinfo is not none -%}{%- set d = d.astimezone(modules.pytz.utc).replace(tzinfo=none) -%}{%- endif -%}
	{{ return("TIMESTAMP '" ~ d.strftime('%Y-%m-%d %H:%M:%S') ~ '.' ~ ('%03d' % (d.microsecond // 1000)) ~ "'") }}
{% endmacro %}

{% macro _window_parse(text) %}
	{#- ISO 8601 text to a datetime: the forms dbt's --event-time flags take, plus fractional
	    seconds and offsets. An invalid string raises Python's own ValueError. -#}
	{{ return(modules.datetime.datetime.fromisoformat(text | string | trim)) }}
{% endmacro %}

{% macro _window_interval(text) %}
	{#- '18 hour', '2 day', '10 days' -> {'amount': '18', 'unit': 'hour'} for a Trino interval -#}
	{%- set parts = (text | string | trim | lower).split() -%}
	{%- if parts | length != 2 or not parts[0].isdigit() -%}
		{%- do _window_error("bad lookback '" ~ text ~ "'; expected '<amount> <unit>' such as '18 hour'") -%}
	{%- endif -%}
	{%- set unit = parts[1].rstrip('s') -%}
	{%- if unit not in ['minute', 'hour', 'day', 'month'] -%}
		{%- do _window_error("bad lookback unit '" ~ parts[1] ~ "'; expected minute, hour, day or month (Trino has no week interval)") -%}
	{%- endif -%}
	{{ return({'amount': parts[0], 'unit': unit}) }}
{% endmacro %}

{% macro _window_rolling_start(lookback) %}
	{%- set lb = _window_interval(lookback) -%}
	{{ return("date_trunc('" ~ lb.unit ~ "', now() - interval '" ~ lb.amount ~ "' " ~ lb.unit ~ ")") }}
{% endmacro %}

{% macro _window_env_pair() %}
	{#- the explicit run window from the environment, or none. The removed DBT_RUN_* names and a
	    --vars pair raise, so a backfill can never fall through to the rolling window unnoticed. -#}
	{%- if (env_var('DBT_RUN_START_DATE', '') or var('DBT_RUN_START_DATE', '') or env_var('DBT_RUN_END_DATE', '') or var('DBT_RUN_END_DATE', '')) | string | trim != '' -%}
		{%- do _window_error('DBT_RUN_START_DATE / DBT_RUN_END_DATE are removed; set DBT_EVENT_TIME_START and DBT_EVENT_TIME_END (half open, both required) instead') -%}
	{%- endif -%}
	{%- if (var('DBT_EVENT_TIME_START', '') ~ var('DBT_EVENT_TIME_END', '')) | trim != '' -%}
		{%- do _window_error('DBT_EVENT_TIME_START / DBT_EVENT_TIME_END are environment variables, not --vars; export them instead') -%}
	{%- endif -%}
	{{ return(_window_pair(env_var('DBT_EVENT_TIME_START', ''), env_var('DBT_EVENT_TIME_END', ''))) }}
{% endmacro %}

{% macro _window_pair(env_start, env_end) %}
	{#- a start/end pair as {'start': literal, 'end': literal}, or none when both are empty -#}
	{%- set s = env_start | string | trim -%}
	{%- set e = env_end | string | trim -%}
	{%- if s == '' and e == '' -%}{{ return(none) }}{%- endif -%}
	{%- if s == '' -%}{%- do _window_error('DBT_EVENT_TIME_END is set but DBT_EVENT_TIME_START is not; set both or none') -%}{%- endif -%}
	{%- if e == '' -%}{%- do _window_error('DBT_EVENT_TIME_START is set but DBT_EVENT_TIME_END is not; set both or none') -%}{%- endif -%}
	{%- set sd = _window_parse(s) -%}
	{%- set ed = _window_parse(e) -%}
	{%- if sd >= ed -%}{%- do _window_error('DBT_EVENT_TIME_START must be before DBT_EVENT_TIME_END') -%}{%- endif -%}
	{{ return({'start': _window_ts_literal(sd), 'end': _window_ts_literal(ed)}) }}
{% endmacro %}

{% macro _window_lookback(lookback) %}
	{#- argument, else the model's meta (empty at parse time), else the job default -#}
	{%- if lookback -%}{{ return(lookback) }}{%- endif -%}
	{%- if config.meta_get('lookback_window') -%}{{ return(config.meta_get('lookback_window')) }}{%- endif -%}
	{{ return((var('DBT_ENV_INCREMENTAL_TIME') | string) ~ ' ' ~ (var('DBT_ENV_INCREMENTAL_TIME_UNIT') | string)) }}
{% endmacro %}

{% macro _window_begin(start_date=none) %}
	{#- the prod first-build floor as a literal: the deprecated start_date argument, the model's
	    `begin`, else the project default -#}
	{{ return(_window_ts_literal(_window_parse(start_date or config.get('begin') or var('DBT_DEFAULT_BEGIN')))) }}
{% endmacro %}

{% macro _window_target_is_prod() %}
	{#- its own macro so a unit test can override it (overrides.macros) and pin the prod branch -#}
	{{ return(target.name == 'prod') }}
{% endmacro %}

{% macro _window_resolve(lookback=none, start_date=none) %}
	{#- {'start': sql, 'end': sql, 'mode': 'batch' | 'run' | 'first_build' | 'rolling'} -#}
	{#- a unit test renders its model's SQL in its own node context, so it counts as a model build.
	    Its overrides.macros must set is_incremental (dbt's rule for unit tests, `this` is unset
	    there) and either _window_target_is_prod or the pair through overrides.env_vars -#}
	{%- set node = model if model is mapping else {} -%}
	{%- set is_model = node.get('resource_type') in ['model', 'unit_test'] -%}
	{%- set batch = node.get('batch') if is_model else none -%}
	{%- if batch -%}
		{{ return({'start': _window_ts_literal(batch.event_time_start), 'end': _window_ts_literal(batch.event_time_end), 'mode': 'batch'}) }}
	{%- endif -%}
	{%- set pair = _window_env_pair() -%}
	{%- if pair is not none -%}
		{{ return({'start': pair.start, 'end': pair.end, 'mode': 'run'}) }}
	{%- endif -%}
	{#- any non-incremental build: a table or view rebuild, --full-refresh, a new table. execute is
	    false while config is parsed, so a config predicate never takes this branch -#}
	{%- if execute and is_model and not is_incremental() -%}
		{%- if not _window_target_is_prod() -%}
			{%- do _window_error('non-incremental build of ' ~ node.get('name') ~ ' on target ' ~ target.name ~ ' needs an explicit window: export DBT_EVENT_TIME_START and DBT_EVENT_TIME_END') -%}
		{%- endif -%}
		{{ return({'start': _window_begin(start_date), 'end': 'now()', 'mode': 'first_build'}) }}
	{%- endif -%}
	{{ return({'start': _window_rolling_start(_window_lookback(lookback)), 'end': 'now()', 'mode': 'rolling'}) }}
{% endmacro %}

{% macro _window_render(start, end, column) %}
	{{ return(column ~ ' >= ' ~ start ~ (' and ' ~ column ~ ' < ' ~ end if end is not none else '')) }}
{% endmacro %}

{% macro incremental_filter(column, start_date=none, lookback=none) %}
	{#- Body read. Half open when a batch or the pair sets an end; open above on a rolling or first
	    build. start_date (second, positional for existing callers) is the deprecated argument form
	    of the `begin` config. -#}
	{%- set w = _window_resolve(lookback=lookback, start_date=start_date) -%}
	{{ return(_window_render(w.start, w.end if w.mode in ['batch', 'run'] else none, column)) }}
{% endmacro %}

{% macro window_start(lookback=none) %}
	{#- The lower bound as SQL: a TIMESTAMP literal, or the rolling date_trunc(...) expression. -#}
	{{ return(_window_resolve(lookback=lookback).start) }}
{% endmacro %}

{% macro window_end() %}
	{#- The upper bound as SQL: a TIMESTAMP literal, or now() on a rolling or first build. -#}
	{{ return(_window_resolve().end) }}
{% endmacro %}
