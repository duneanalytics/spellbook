{% macro optimize_tables() %}
{%- if flags.full_refresh -%}
{% set sql_1 %}
OPTIMIZE dbt_thomas_transfers_ethereum.erc20_agg_hour;
{% endset %}

{% set sql_2 %}
OPTIMIZE transfers_ethereum.erc20_agg_day;
{% endset %}

{% set sql_3 %}
OPTIMIZE opensea_ethereum.trades;
{% endset %}

{% set sql_4 %}
OPTIMIZE opensea_solana.trades;
{% endset %}

{% set sql_5 %}
OPTIMIZE magiceden_solana.trades;
{% endset %}

{% do run_query(sql_1) %}
{% do run_query(sql_2) %}
{% do run_query(sql_3) %}
{% do run_query(sql_4) %}
{% do run_query(sql_5) %}

{% do log("Tables Optimized", info=True) %}
{%- else -%}
{%- endif -%}
{% endmacro %}