{% macro optimize_spell(this, materialization) %}
{%- if target.name == 'prod' and materialization in ('table', 'incremental') -%}
        OPTIMIZE {{this}};
{%- else -%}
{%- endif -%}
{%- endmacro -%}
