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

{% set opensea_events %}
OPTIMIZE opensea.events;
{% endset %}

{% set seaport_ethereum_transfers %}
OPTIMIZE seaport_ethereum.transfers;
{% endset %}

{% set magiceden_events %}
OPTIMIZE magiceden.events;
{% endset %}

{% set nft_events %}
OPTIMIZE nft.events;
{% endset %}

{% set nft_trades %}
OPTIMIZE nft.trades;
{% endset %}

{% set nft_mints %}
OPTIMIZE nft.mints;
{% endset %}

{% set nft_burns %}
OPTIMIZE nft.burns;
{% endset %}

{% set nft_fees %}
OPTIMIZE nft.fees;
{% endset %}

{% set uniswap_v1_ethereum_trades %}
OPTIMIZE uniswap_v1_ethereum.trades;
{% endset %}

{% set uniswap_v2_ethereum_trades %}
OPTIMIZE uniswap_v2_ethereum.trades;
{% endset %}

{% set sudoswap_ethereum_events %}
OPTIMIZE sudoswap_ethereum.events;
{% endset %}

{% set safe_safes %}
OPTIMIZE safe.safes;
{% endset %}

{% set safe_eth_transfers %}
OPTIMIZE safe.eth_transfers;
{% endset %}

{% do run_query(transfers_ethereum_erc20_agg_hour) %}
{% do run_query(transfers_ethereum_erc20_agg_day) %}
{% do run_query(transfers_ethereum_erc721_agg_hour) %}
{% do run_query(transfers_ethereum_erc721_agg_day) %}
{% do run_query(transfers_ethereum_erc1155_agg_hour) %}
{% do run_query(transfers_ethereum_erc1155_agg_day) %}
{% do run_query(opensea_events) %}
{% do run_query(seaport_ethereum_transfers) %}
{% do run_query(magiceden_events) %}
{% do run_query(nft_events) %}
{% do run_query(nft_trades) %}
{% do run_query(nft_mints) %}
{% do run_query(nft_burns) %}
{% do run_query(nft_fees) %}
{% do run_query(uniswap_v1_ethereum_trades) %}
{% do run_query(uniswap_v2_ethereum_trades) %}
{% do run_query(sudoswap_ethereum_events) %}
{% do run_query(safe_safes) %}
{% do run_query(safe_eth_transfers) %}


{% do log("Tables Optimized", info=True) %}
{%- else -%}
{%- endif -%}
{% endmacro %}