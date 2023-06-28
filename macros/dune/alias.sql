{% macro alias(alias_name, legacy_model=False) %}
  {% set legacy = target.type != 'trino' %}
  {%- if legacy and not legacy_model -%}
    {% do return(alias_name + '_dunesql') %}
  {%- endif -%}
  {%- if not legacy and legacy_model -%}
    {% do return(alias_name + '_legacy') %}
  {%- endif -%}
  {% do return(alias_name) %}
{% endmacro %}