{#-
  optimize_spell: TEMPORARILY DISABLED (CUR2-3191).

  Inline `ALTER TABLE ... EXECUTE optimize` has been turned off across all dbt
  jobs and sqlmesh. Compaction is being handed to DuneCP's autonomous table
  health-check scan, which health-checks, analyzes and optimizes the
  trino-owned spell tables on its own cadence -- so running OPTIMIZE
  synchronously in the dbt post-hook is redundant and only adds latency to the
  model build's critical path.

  This is a reversible off-switch: the macro is still wired into every
  subproject post-hook but now emits nothing. Re-evaluate after ~1 week; then
  either delete the macro and its post-hook calls for good, or revert this
  commit to restore the previous row-gated behavior.
-#}
{% macro optimize_spell(this, materialization) %}
{#- no-op: inline OPTIMIZE disabled; DuneCP health-check owns compaction (CUR2-3191) -#}
{%- endmacro -%}
