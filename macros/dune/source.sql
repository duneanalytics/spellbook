{% macro source(source_name, table_name) %}
  {% set rel = builtins.source(source_name, table_name) %}
  {%- if env_var('DBT_DUNE_SQL', 'False') == 'True' -%}
    {% set newrel = rel.replace_path(database="delta_prod") %}
    {% do return(newrel) %}
  {%- else -%}
    {% do return(rel) %}
  {%- endif -%}
{% endmacro %}