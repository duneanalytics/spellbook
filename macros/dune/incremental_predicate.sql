{% macro incremental_predicate(column) -%}
{{column}} >= date_trunc('{{var("DBT_ENV_INCREMENTAL_TIME_UNIT")}}', now() - interval '{{var('DBT_ENV_INCREMENTAL_TIME')}}' {{var('DBT_ENV_INCREMENTAL_TIME_UNIT')}})
{%- endmacro -%}
