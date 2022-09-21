{% macro optimize_tables() %}
{% set looksrare_ethereum_events %}
OPTIMIZE looksrare_ethereum.events;
{% endset %}

{% set magiceden_solana_events %}
OPTIMIZE magiceden_solana.events;
{% endset %}

{% set opensea_solana_events %}
OPTIMIZE opensea_solana.events;
{% endset %}

{% set opensea_v1_ethereum_events %}
OPTIMIZE opensea_v1_ethereum.events;
{% endset %}

{% set safe_ethereum_eth_transfers %}
OPTIMIZE safe_ethereum.eth_transfers;
{% endset %}

{% set safe_ethereum_safes %}
OPTIMIZE safe_ethereum.safes;
{% endset %}

{% set seaport_ethereum_transfers %}
OPTIMIZE seaport_ethereum.transfers;
{% endset %}

{% set sudoswap_ethereum_events %}
OPTIMIZE sudoswap_ethereum.events;
{% endset %}

{% set cryptopunks_ethereum_events %}
OPTIMIZE cryptopunks_ethereum.events;
{% endset %}

{% set tornado_cash_deposits %}
OPTIMIZE tornado_cash.deposits;
{% endset %}

{% set tornado_cash_withdrawals %}
OPTIMIZE tornado_cash.withdrawals;
{% endset %}

{% set transfers_ethereum_erc20_agg_day %}
OPTIMIZE transfers_ethereum.erc20_agg_day;
{% endset %}

{% set transfers_ethereum_erc20_agg_hour %}
OPTIMIZE transfers_ethereum.erc20_agg_hour;
{% endset %}

{% set transfers_ethereum_erc721_agg_day %}
OPTIMIZE transfers_ethereum.erc721_agg_day;
{% endset %}

{% set transfers_ethereum_erc721_agg_hour %}
OPTIMIZE transfers_ethereum.erc721_agg_hour;
{% endset %}

{% set transfers_ethereum_erc1155_agg_day %}
OPTIMIZE transfers_ethereum.erc1155_agg_day;
{% endset %}

{% set transfers_ethereum_erc1155_agg_hour %}
OPTIMIZE transfers_ethereum.erc1155_agg_hour;
{% endset %}

{% set uniswap_v1_ethereum_trades %}
OPTIMIZE uniswap_v1_ethereum.trades;
{% endset %}

{% set uniswap_v2_ethereum_trades %}
OPTIMIZE uniswap_v2_ethereum.trades;
{% endset %}

{% set uniswap_v1_ethereum_trades %}
OPTIMIZE uniswap_v1_ethereum.trades;
{% endset %}

{% set uniswap_v2_ethereum_trades %}
OPTIMIZE uniswap_v2_ethereum.trades;
{% endset %}

{% set x2y2_ethereum_events %}
OPTIMIZE x2y2_ethereum.events;
{% endset %}

{% set ens_resolver_addresses %}
OPTIMIZE ens.resolver_addresses;
{% endset %}

{% set archipelago_ethereum_events %}
OPTIMIZE archipelago_ethereum.events;
{% endset %}

{% set foundation_ethereum_events %}
OPTIMIZE foundation_ethereum.events;
{% endset %}

{% set labels_all %}
OPTIMIZE labels.all;
{% endset %}

{% set cow_protocol_ethereum_trades %}
OPTIMIZE cow_protocol_ethereum.trades;
{% endset %}

{% set cow_protocol_ethereum_batches %}
OPTIMIZE cow_protocol_ethereum.batches;
{% endset %}


{% do run_query(looksrare_ethereum_events) %}
{% do run_query(magiceden_solana_events) %}
{% do run_query(opensea_solana_events) %}
{% do run_query(opensea_v1_ethereum_events) %}
{% do run_query(safe_ethereum_eth_transfers) %}
{% do run_query(safe_ethereum_safes) %}
{% do run_query(ens_resolver_addresses) %}
{% do run_query(seaport_ethereum_transfers) %}
{% do run_query(sudoswap_ethereum_events) %}
{% do run_query(cryptopunks_ethereum_events) %}
{% do run_query(tornado_cash_deposits) %}
{% do run_query(tornado_cash_withdrawals) %}
{% do run_query(transfers_ethereum_erc20_agg_hour) %}
{% do run_query(transfers_ethereum_erc20_agg_day) %}
{% do run_query(transfers_ethereum_erc721_agg_hour) %}
{% do run_query(transfers_ethereum_erc721_agg_day) %}
{% do run_query(transfers_ethereum_erc1155_agg_hour) %}
{% do run_query(transfers_ethereum_erc1155_agg_day) %}
{% do run_query(uniswap_v1_ethereum_trades) %}
{% do run_query(uniswap_v2_ethereum_trades) %}
{% do run_query(x2y2_ethereum_events) %}
{% do run_query(archipelago_ethereum_events) %}
{% do run_query(foundation_ethereum_events) %}
{% do run_query(labels_all) %}
{% do run_query(cow_protocol_ethereum_trades) %}
{% do run_query(cow_protocol_ethereum_batches) %}
{% do log("Tables Optimized", info=True) %}
{% endmacro %}
