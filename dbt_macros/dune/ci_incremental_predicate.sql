{% macro ci_incremental_predicate(column) -%}
{%- if env_var('CI', false) | as_bool and env_var('CI_LIMITED_RANGE', false) | as_bool -%}
    -- In CI environment, only process data from the last specified days
    {{column}} >= date_trunc('day', now() - interval '{{ env_var("CI_DATE_RANGE", "30") }}' day)
{%- else -%}
    -- In production environment, use the original incremental logic
    {{column}} >= date_trunc('{{var("DBT_ENV_INCREMENTAL_TIME_UNIT")}}', now() - interval '{{var('DBT_ENV_INCREMENTAL_TIME')}}' {{var('DBT_ENV_INCREMENTAL_TIME_UNIT')}})
{%- endif -%}
{%- endmacro -%} 