{% macro get_source(node) -%}
    {{ return(adapter.dispatch('get_source')(node)) }}
{%- endmacro %}


{% macro default__get_source(node) -%}
    'hi'
{%- endmacro %}