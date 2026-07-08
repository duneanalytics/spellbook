{% macro incremental_month_predicate(column) -%}
{{column}} >= date_trunc('month', now() - interval '{{var('DBT_ENV_INCREMENTAL_TIME')}}' {{var('DBT_ENV_INCREMENTAL_TIME_UNIT')}})
{%- endmacro -%}
