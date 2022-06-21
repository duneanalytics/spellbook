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

{% set transfers_ethereum_erc1155_agg_hour %}
OPTIMIZE transfers_ethereum.erc1155_agg_hour;
{% endset %}

{% set transfers_ethereum_erc1155_agg_day %}
OPTIMIZE transfers_ethereum.erc1155_agg_day;
{% endset %}

{% set opensea_trades %}
OPTIMIZE opensea.trades;
{% endset %}

{% set magiceden_trades %}
OPTIMIZE magiceden.trades;
{% endset %}

{% set uniswap_trades %}
OPTIMIZE uniswap.trades;
{% endset %}

{% set nft_trades %}
OPTIMIZE nft.trades;
{% endset %}


{% do run_query(transfers_ethereum_erc20_agg_hour) %}
{% do run_query(transfers_ethereum_erc20_agg_day) %}
{% do run_query(transfers_ethereum_erc721_agg_hour) %}
{% do run_query(transfers_ethereum_erc721_agg_day) %}
{% do run_query(transfers_ethereum_erc1155_agg_hour) %}
{% do run_query(transfers_ethereum_erc1155_agg_day) %}
{% do run_query(opensea_trades) %}
{% do run_query(magiceden_trades) %}
{% do run_query(uniswap_trades) %}
{% do run_query(nft_trades) %}

{% do log("Tables Optimized", info=True) %}
{%- else -%}
{%- endif -%}
{% endmacro %}