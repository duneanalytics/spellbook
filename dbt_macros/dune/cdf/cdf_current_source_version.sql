{#-
  Current max committed Delta version of a spell source, via its "$history" table.
  Cheap for spell sources (one commit per cadence run); NOT safe for high-write raw
  tables (Phase 2 uses an error-sentinel probe instead). Used to bootstrap the
  watermark and to pin the bootstrap snapshot (FOR VERSION AS OF V). Returns an int
  or none. Guarded for parse mode.
-#}
{% macro cdf_current_source_version(base_relation) %}
  {%- if not execute -%}
    {{ return(none) }}
  {%- endif -%}
  {%- set probe -%}
    select max(version) as v
    from {{ base_relation.database }}.{{ base_relation.schema }}."{{ base_relation.identifier }}$history"
  {%- endset -%}
  {%- set results = run_query(probe) -%}
  {%- if results is not none and results.rows | length > 0 and results.rows[0][0] is not none -%}
    {{ return(results.rows[0][0] | int) }}
  {%- else -%}
    {{ return(none) }}
  {%- endif -%}
{% endmacro %}
