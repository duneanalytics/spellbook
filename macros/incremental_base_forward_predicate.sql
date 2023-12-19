{% macro incremental_base_forward_predicate(column, base_time, days_forward) -%}
        {{column}} BETWEEN date_trunc('{{var("DBT_ENV_INCREMENTAL_TIME_UNIT")}}', base_time - interval '{{var('DBT_ENV_INCREMENTAL_TIME')}}' {{var('DBT_ENV_INCREMENTAL_TIME_UNIT')}}) AND date_trunc('{{var("DBT_ENV_INCREMENTAL_TIME_UNIT")}}', base_time + interval '{{days_forward}}' day)
{%- endmacro -%}
