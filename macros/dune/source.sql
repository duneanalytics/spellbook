{% macro source(source_name, table_name) %}
  {% set rel = builtins.source(source_name, table_name) %}
  {%- if target.type == 'trino' and rel.database != 'hive'-%}
    {% set newrel = rel.replace_path(database="delta_prod") %}
    {% do return(newrel) %}
  {%- else -%}
    {% do return(rel) %}
  {%- endif -%}
{% endmacro %}
