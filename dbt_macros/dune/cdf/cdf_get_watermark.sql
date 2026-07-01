{#-
  Read the stored CDF watermark (dune.cdf.source_version) from a target relation's
  Delta table properties. Returns the integer source version W, or none when absent
  (the caller should then bootstrap). Trino-Delta specific: reads the "$properties"
  metadata table, which exposes the Delta metaData.configuration map (where Dune's
  extra_properties land). Guarded for parse mode (no introspective query).
-#}
{% macro cdf_get_watermark(target_relation) %}
  {%- if not execute -%}
    {{ return(none) }}
  {%- endif -%}
  {%- set probe -%}
    select value
    from {{ target_relation.database }}.{{ target_relation.schema }}."{{ target_relation.identifier }}$properties"
    where key = 'dune.cdf.source_version'
  {%- endset -%}
  {%- set results = run_query(probe) -%}
  {%- if results is not none and results.rows | length > 0 and results.rows[0][0] is not none -%}
    {{ return(results.rows[0][0] | int) }}
  {%- else -%}
    {{ return(none) }}
  {%- endif -%}
{% endmacro %}
