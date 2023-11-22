{% macro source(source_name, table_name, cross_project = False) %}
  {% set rel = builtins.source(source_name, table_name) %}
  {%- if target.type == 'trino' and not cross_project -%}
    {% set newrel = rel.replace_path(database="delta_prod") %}
    {% do return(newrel) %}
  {%- elif cross_project -%}
    {% set newrel = rel.replace_path(database="hive") %}
    {% do return(newrel) %}
  {%- else -%}
    {% do return(rel) %}
  {%- endif -%}
{% endmacro %}
