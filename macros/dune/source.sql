{% macro source(source_name, table_name) %}

  {% set rel = builtins.source(source_name, table_name) %}
  {% set newrel = rel.replace_path(database="delta_prod") %}
  {% do return(newrel) %}

{% endmacro %}