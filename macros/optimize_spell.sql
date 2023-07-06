{% macro optimize_spell(this, materialization) %}
{%- if target.name == 'prod' and materialization in ('table', 'incremental') -%}
  {%- if target.type == 'trino' -%}
    ALTER TABLE {{this}} EXECUTE optimize
  {%- else -%}
     OPTIMIZE {{this}};
  {%- endif -%}
{%- else -%}
{%- endif -%}
{%- endmacro -%}
