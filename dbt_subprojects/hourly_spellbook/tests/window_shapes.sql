{#
  Render check for the window macro family (dbt_macros/dune/window.sql) and the rolling output of
  incremental_predicate, which wraps it. A singular test, so the existing `dbt test` step runs it
  (state:new, then state:modified.macros whenever a window macro changes). Ported from
  curated-data tests/window_shapes.sql; one copy per subproject, since each sets its own
  DBT_ENV_INCREMENTAL_TIME default.

  Every row is one rendered shape: the helpers get explicit inputs, the public macros and the
  wrapper render in this test's context (no model, no pair: rolling mode), which is the output
  the incremental_predicate callers rely on. The test fails on any row whose rendering differs.
#}
{%- set dt = modules.datetime.datetime -%}
{%- set unit = var('DBT_ENV_INCREMENTAL_TIME_UNIT') -%}
{%- set amount = var('DBT_ENV_INCREMENTAL_TIME') -%}
{%- set rolling = "date_trunc('" ~ unit ~ "', now() - interval '" ~ amount ~ "' " ~ unit ~ ")" -%}
{%- set rows = [] -%}

{# literals #}
{%- do rows.append(['literal date', _window_ts_literal(_window_parse('2024-01-05')), "TIMESTAMP '2024-01-05 00:00:00.000'"]) -%}
{%- do rows.append(['literal time', _window_ts_literal(_window_parse('2024-01-05 06:30:00')), "TIMESTAMP '2024-01-05 06:30:00.000'"]) -%}
{%- do rows.append(['literal ms tz', _window_ts_literal(modules.pytz.utc.localize(dt(2024, 3, 15, 10, 20, 30, 456000))), "TIMESTAMP '2024-03-15 10:20:30.456'"]) -%}

{# the pair #}
{%- set pair = _window_pair('2024-01-01', '2024-01-05') -%}
{%- do rows.append(['pair start', pair.start, "TIMESTAMP '2024-01-01 00:00:00.000'"]) -%}
{%- do rows.append(['pair end', pair.end, "TIMESTAMP '2024-01-05 00:00:00.000'"]) -%}
{%- do rows.append(['pair none', _window_pair('', ''), 'None']) -%}

{# rolling expression and lookback parsing #}
{%- do rows.append(['rolling default', _window_rolling_start('3 day'), "date_trunc('day', now() - interval '3' day)"]) -%}
{%- do rows.append(['rolling 18 hour', _window_rolling_start('18 hour'), "date_trunc('hour', now() - interval '18' hour)"]) -%}
{%- do rows.append(['lookback job default', _window_lookback(none), amount ~ ' ' ~ unit]) -%}

{# the public accessors and the older macros render in this test's context (no model). Their
   output depends on the run inputs, so the expectation follows them: with the env pair set,
   the accessors must render exactly the pair; with no explicit input, the rolling output; with
   the legacy DBT_RUN_* vars set the accessors raise by design, so nothing is pinned here. #}
{%- set env_start = env_var('DBT_EVENT_TIME_START', '') | trim -%}
{%- set env_end = env_var('DBT_EVENT_TIME_END', '') | trim -%}
{%- set legacy_set = (env_var('DBT_RUN_START_DATE', '') or var('DBT_RUN_START_DATE', '') or env_var('DBT_RUN_END_DATE', '') or var('DBT_RUN_END_DATE', '')) | trim != '' -%}
{%- if env_start != '' or env_end != '' -%}
{%- set live = _window_pair(env_start, env_end) -%}
{%- do rows.append(['window_start pair', window_start(), live.start]) -%}
{%- do rows.append(['window_start lookback pair', window_start(lookback='18 hour'), live.start]) -%}
{%- do rows.append(['window_end pair', window_end(), live.end]) -%}
{%- do rows.append(['incremental_filter pair', incremental_filter('ts'), 'ts >= ' ~ live.start ~ ' and ts < ' ~ live.end]) -%}
{%- do rows.append(['incremental_predicate pair', incremental_predicate('ts'), 'ts >= ' ~ live.start ~ ' and ts < ' ~ live.end]) -%}
{%- elif not legacy_set -%}
{%- do rows.append(['incremental_filter rolling', incremental_filter('ts'), 'ts >= ' ~ rolling]) -%}
{%- do rows.append(['incremental_filter lookback', incremental_filter('ts', lookback='18 hour'), "ts >= date_trunc('hour', now() - interval '18' hour)"]) -%}
{%- do rows.append(['window_start rolling', window_start(), rolling]) -%}
{%- do rows.append(['window_start lookback', window_start(lookback='18 hour'), "date_trunc('hour', now() - interval '18' hour)"]) -%}
{%- do rows.append(['window_end rolling', window_end(), 'now()']) -%}
{%- do rows.append(['incremental_predicate rolling', incremental_predicate('DBT_INTERNAL_DEST.day'), 'DBT_INTERNAL_DEST.day >= ' ~ rolling]) -%}
{#- the exact text the pre-window incremental_predicate rendered, so callers see no change -#}
{%- do rows.append(['incremental_predicate legacy text', incremental_predicate('t.block_time'), "t.block_time >= date_trunc('" ~ unit ~ "', now() - interval '" ~ amount ~ "' " ~ unit ~ ")"]) -%}
{%- endif -%}

select label, got, want
from (
	values
	{%- for r in rows %}
	('{{ r[0] }}', '{{ (r[1] | string | trim) | replace("'", "''") }}', '{{ (r[2] | string | trim) | replace("'", "''") }}'){{ ',' if not loop.last }}
	{%- endfor %}
) as t (label, got, want)
where got <> want
