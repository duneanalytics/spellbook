{% macro optimize_tables() %}
{%- if flags.full_refresh -%}
{% set transfers_ethereum_erc20_agg_hour %}
OPTIMIZE transfers_ethereum.erc20_agg_hour;
{% endset %}

{% set transfers_ethereum_erc20_agg_day %}
OPTIMIZE transfers_ethereum.erc20_agg_day;
{% endset %}

{% set transfers_ethereum_erc721_agg_hour %}
OPTIMIZE transfers_ethereum.erc721_agg_hour;
{% endset %}

{% set transfers_ethereum_erc721_agg_day %}
OPTIMIZE transfers_ethereum.erc721_agg_day;
{% endset %}

{% set opensea_ethereum_trades %}
OPTIMIZE opensea_ethereum.trades;
{% endset %}

{% set opensea_solana_trades %}
OPTIMIZE opensea_solana.trades;
{% endset %}

{% set magiceden_solana_trades %}
OPTIMIZE magiceden_solana.trades;
{% endset %}

{% set uniswap_v2_ethereum_trades %}
OPTIMIZE uniswap_v2_ethereum.trades;
{% endset %}

{% set uniswap_v3_ethereum_trades %}
OPTIMIZE uniswap_v3_ethereum.trades;
{% endset %}

{% do run_query(transfers_ethereum_erc20_agg_hour) %}
{% do run_query(transfers_ethereum_erc20_agg_day) %}
{% do run_query(transfers_ethereum_erc721_agg_hour) %}
{% do run_query(transfers_ethereum_erc721_agg_day) %}
{% do run_query(opensea_ethereum_trades) %}
{% do run_query(opensea_solana_trades) %}
{% do run_query(magiceden_solana_trades) %}
{% do run_query(uniswap_v2_ethereum_trades) %}
{% do run_query(uniswap_v3_ethereum_trades) %}

{% do log("Tables Optimized", info=True) %}
{%- else -%}
{%- endif -%}
{% endmacro %}