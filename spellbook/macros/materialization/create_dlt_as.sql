

{% macro get_create_dlt_as_sql(relation, sql) -%}
  {{ adapter.dispatch('get_create_dlt_as_sql', 'dbt')(relation, sql) }}
{%- endmacro %}

{% macro default__get_create_dlt_as_sql(relation, sql) -%}
  {{ return(create_dlt_as(relation, sql)) }}
{% endmacro %}


{% macro create_dlt_as(relation, sql) -%}

    CREATE OR REPLACE TABLE delta.{{ relation }} USING DELTA as (
    {{ sql }})

{%- endmacro %}