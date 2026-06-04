{#-
  optimize_spell: post-hook that runs Delta-Lake OPTIMIZE on a materialized
  model, conditional on the most recent main statement having affected enough
  rows to make compaction worthwhile.

  Background: prior versions ran `ALTER TABLE ... EXECUTE optimize` on every
  prod incremental model regardless of whether the MERGE wrote any rows. About
  one in four incremental cadence runs writes 0 rows (no new source data, no
  late-arriving rows in the lookback window), making OPTIMIZE pure overhead.
  Measured across recent cadence runs (Hourly, Tokens, Daily, DEX, Solana),
  the OPTIMIZE post-hook accounts for an estimated ~30% of total dbt execute
  time, and ~12% is burned on zero-row models alone.

  This macro now skips OPTIMIZE when `rows_affected < OPTIMIZE_MIN_ROWS`
  (default 1000) for incremental models. The threshold is overridable
  per-invocation via `--vars '{OPTIMIZE_MIN_ROWS: N}'` or per-model via the
  `optimize_min_rows` config.

  `table` materialization is unchanged: CTAS replaces the table wholesale,
  so we keep the conservative always-optimize behavior there for now.

  Small-file accumulation on rarely-touched tables is mitigated by Delta's
  internal compaction and can be addressed by a separate, scheduled OPTIMIZE
  pass (out of scope for this PR).

  Testing affordance: setting `OPTIMIZE_SPELL_FORCE=true` enables the macro
  on non-prod targets so the conditional logic can be exercised against a
  personal schema. Default is false (behavior unchanged on dev/ci targets).
-#}
{% macro optimize_spell(this, materialization) %}
{%- set is_optimize_target = (target.name == 'prod') or var('OPTIMIZE_SPELL_FORCE', false) -%}
{%- if not is_optimize_target -%}
  {#- non-prod (and OPTIMIZE_SPELL_FORCE not set): no-op (unchanged) -#}
{%- elif materialization not in ('table', 'incremental') -%}
  {#- views and other materializations: nothing to compact -#}
{%- elif materialization == 'table' -%}
  {#- full refresh table: keep existing always-optimize behavior -#}
  {%- if target.type == 'trino' -%}
    ALTER TABLE {{ this }} EXECUTE optimize
  {%- else -%}
    OPTIMIZE {{ this }};
  {%- endif -%}
{%- else -%}
  {#- incremental: gate on rows actually written by the main statement -#}
  {%- set main_result = load_result('main') -%}
  {%- set rows_affected = 0 -%}
  {%- if main_result is not none and main_result.response is not none and main_result.response.rows_affected is not none -%}
    {%- set rows_affected = main_result.response.rows_affected | int -%}
  {%- endif -%}
  {%- set min_rows = config.get('optimize_min_rows', var('OPTIMIZE_MIN_ROWS', 1000)) | int -%}
  {%- if rows_affected >= min_rows -%}
    {%- if target.type == 'trino' -%}
      ALTER TABLE {{ this }} EXECUTE optimize
    {%- else -%}
      OPTIMIZE {{ this }};
    {%- endif -%}
  {%- endif -%}
{%- endif -%}
{%- endmacro -%}
