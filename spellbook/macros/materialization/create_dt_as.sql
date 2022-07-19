

{% macro get_create_dt_as_sql(file_path, sql) -%}
  {{ adapter.dispatch('get_create_dt_as_sql', 'dbt')(file_path, sql) }}
{%- endmacro %}

{% macro default__get_create_dt_as_sql(file_path, sql) -%}
  {{ return(create_dt_as(file_path, sql)) }}
{% endmacro %}


{% macro create_dt_as(file_path, sql) -%}

    CREATE OR REPLACE TABLE delta.{{ file_path }} USING DELTA as (
    {{ sql }})

{%- endmacro %}