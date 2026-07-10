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

  Frequency throttle: on the ~hourly cadence subprojects each hot table was
  being re-compacted on every run (~26x/day), which is redundant since each
  OPTIMIZE rewrites files that the next run's small writes immediately re-
  fragment. For those subprojects (dex, tokens, hourly_spellbook) each model now
  OPTIMIZEs at most once per UTC day, on the single cadence run whose hour
  matches a stable per-model slot derived from a hash of the relation name. This
  mirrors the throttle already used in spellbook-sqlmesh. See optimize_due_today
  below for the slot logic and for why the subproject scope is hardcoded here
  rather than opted into via each dbt_project.yml.

  `table` materialization always OPTIMIZEs on its slot run: CTAS replaces the
  table wholesale, so the compaction keeps file counts low for downstream reads.

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
{%- elif not optimize_due_today(this) -%}
  {#- frequency throttle: not this model's daily OPTIMIZE slot -#}
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

{#-
  optimize_due_today: stateless ~1/day OPTIMIZE throttle for one relation.

  WHY THE SUBPROJECT SCOPE IS HARDCODED HERE (not a dbt_project.yml var):
  optimize_spell is a project-level post-hook, so it sits in every model's macro
  dependency set. Spellbook CI builds `state:modified.macros`
  (.github/workflows/dbt_run.yml), and CI only selects a subproject when a file
  under dbt_subprojects/<p>/ changes. So adding an opt-in var to each
  dbt_project.yml would select those subprojects and then flag 100% of their
  models as modified -> a full-subproject rebuild in CI for a change that only
  alters OPTIMIZE timing, never model output. Changes confined to
  dbt_macros/dune/** are CI-build-exempt by design (see project selection in
  dbt_full_run.yml / dbt_pr_trigger.yml), so keeping the scope here ships this
  as a macro-only change with no wasteful cascade. It takes effect on the next
  prod cadence run after merge; no rebuild needed.

  Slot logic: returns true (OPTIMIZE proceeds) unless the running project is a
  throttled ~hourly subproject, in which case it returns true only on the
  cadence run whose UTC hour equals the model's stable slot -- the first 8 hex
  chars of md5(relation) mod 24 -- spreading models across the 24 hourly runs so
  the load is smeared rather than spiked. daily_spellbook (1 run/day) and solana
  (~5 runs/day) are excluded: an hourly slot would rarely be hit.
  `OPTIMIZE_SLOT_BYPASS=true` forces true for one invocation (tests). Fail-safe:
  if project_name is unresolved, defaults to true so compaction is never
  silently disabled.
-#}
{% macro optimize_due_today(this) %}
{%- set throttled_projects = ['dex', 'tokens', 'hourly_spellbook'] -%}
{%- if project_name is not defined or project_name not in throttled_projects or var('OPTIMIZE_SLOT_BYPASS', false) -%}
  {{ return(true) }}
{%- endif -%}
{%- set slot = (local_md5(this | string)[:8] | int(0, 16)) % 24 -%}
{{ return(run_started_at.hour == slot) }}
{%- endmacro -%}
