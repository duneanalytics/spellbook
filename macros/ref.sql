{% macro ref(model_name) %}

  {% set rel = builtins.ref(model_name) %}
  {% set newrel = rel.replace_path(database="delta_prod") %}
  {% do return(newrel) %}

{% endmacro %}