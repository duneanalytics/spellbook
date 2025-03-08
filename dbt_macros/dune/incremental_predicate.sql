{% macro incremental_predicate(column) -%}
{{ ci_incremental_predicate(column) }}
{%- endmacro -%}
