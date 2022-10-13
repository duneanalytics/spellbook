{% macro optimize_spell(this, materialization) %}
{%- if target.name == 'prod' and materialization in ('table', 'incremental') -%}
        CACHE TABLE {{this}};
{%- else -%}
{%- endif -%}
{%- endmacro -%}