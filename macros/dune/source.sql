{% macro source(source_name, table_name, dune=False) %}
  {% set rel = builtins.source(source_name, table_name) %}
  {%- if dune -%}
      {% set newrel = rel.replace_path(database="dune") %}
      {% do return(newrel) %}
  {%- elif target.type == "trino" -%}
      {% set newrel = rel.replace_path(database="delta_prod") %}
      {% do return(newrel) %}
  {%- else -%}
      {% do return(rel) %}
  {%- endif -%}
{% endmacro %}