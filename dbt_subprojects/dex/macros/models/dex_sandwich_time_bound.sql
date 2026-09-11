{# Time bound for the trade and transaction inputs of dex_sandwiches / dex_sandwiched.

   CI creates these tables from scratch, so its first build is not incremental and would scan
   full history; on BNB that exceeds the job timeout. Bound it the same way dex_trades and
   dex_base_trades_macro do, with a fixed CI window rather than incremental_predicate, so the
   CI build size stays independent of DBT_ENV_INCREMENTAL_TIME. Production full refresh emits
   no bound at all -- callers only render this when bounded_run is true. #}

{% macro dex_sandwich_time_bound(column) -%}
{%- if target.name == 'ci' -%}
{{ column }} > current_date - interval '7' day
{%- else -%}
{{ incremental_predicate(column) }}
{%- endif -%}
{%- endmacro %}
