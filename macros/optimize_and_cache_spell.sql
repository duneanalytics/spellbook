{% macro optimize_and_cache_spell(this, materialization) %}
{%- if target.name == 'prod' and materialization in ('table', 'incremental') -%}
        OPTIMIZE {{this}};
        CACHE TABLE {{this}};
{%- else -%}
{%- endif -%}
{%- endmacro -%}
