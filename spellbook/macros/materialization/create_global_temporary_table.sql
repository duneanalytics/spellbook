

{% macro get_create_global_temp_view_as_sql(relation, sql) -%}

  {{ adapter.dispatch('get_create_global_temp_view_as_sql', 'dbt')(relation, sql) }}

{%- endmacro %}

{% macro default__get_create_global_temp_view_as_sql(relation, sql) -%}

  {{ return(create_global_temp_view(relation, sql)) }}

{% endmacro %}

{% macro create_global_temp_view(relation, sql) -%}

    CREATE OR REPLACE GLOBAL TEMPORARY VIEW {{ relation }} as (
    {{ sql }})

{%- endmacro %}