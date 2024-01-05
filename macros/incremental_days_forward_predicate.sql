{% macro incremental_days_forward_predicate(column, base_time, days_forward, interval_period) -%}
        {{column}} BETWEEN date_trunc('day', base_time - interval '{{var('DBT_ENV_INCREMENTAL_TIME')}}' day) AND date_trunc('day', base_time + interval '{{days_forward}}' day)
{%- endmacro -%}
