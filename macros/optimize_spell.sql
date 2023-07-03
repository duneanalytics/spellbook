{% macro optimize_spell(this, materialization) %}
{%- if target.name == 'prod' and target.type != 'trino' and materialization in ('table', 'incremental') -%}
        OPTIMIZE {{this}};
{%- else -%}
{%- endif -%}
{%- endmacro -%}
