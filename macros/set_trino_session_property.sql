{% macro set_trino_session_property(enabled, property, value) %}
{%- if enabled and target.type == 'trino'-%}
  SET SESSION {{property}}={{value if value is boolean else "'%s'" % value}}
{%- endif -%}
{%- endmacro -%}


{%- macro is_materialized(model) -%}
  {% do return(model.config.materialized in ('table', 'incremental')) %}
{%- endmacro -%}