{{ config(
    schema = 'sushiswap_agg',
    alias = 'trades',
    materialized = 'view',
    post_hook='{{ expose_spells(
        blockchains   = \'["arbitrum","avalanche_c","apechain","base","blast","bnb","celo","ethereum","fantom","gnosis","linea","mantle","nova","optimism","polygon","scroll","sonic","zkevm","zksync"]\',
        spell_type    = "project",
        spell_name    = "sushiswap_agg",
        contributors  = \'["olastenberg"]\'
    ) }}'
) }}


{% set sushiswap_agg_models = [
    ref('sushiswap_agg_arbitrum_trades'),
    ref('sushiswap_agg_avalanche_c_trades'),
    ref('sushiswap_agg_apechain_trades'),
    ref('sushiswap_agg_base_trades'),
    ref('sushiswap_agg_blast_trades'),
    ref('sushiswap_agg_bnb_trades'),
    ref('sushiswap_agg_celo_trades'),
    ref('sushiswap_agg_ethereum_trades'),
    ref('sushiswap_agg_fantom_trades'),
    ref('sushiswap_agg_gnosis_trades'),
    ref('sushiswap_agg_linea_trades'),
    ref('sushiswap_agg_mantle_trades'),
    ref('sushiswap_agg_nova_trades'),
    ref('sushiswap_agg_optimism_trades'),
    ref('sushiswap_agg_polygon_trades'),
    ref('sushiswap_agg_scroll_trades'),
    ref('sushiswap_agg_sonic_trades'),
    ref('sushiswap_agg_zkevm_trades'),
    ref('sushiswap_agg_zksync_trades')
] %}

select *
from (
  {% for dex_model in sushiswap_agg_models %}
    select
      blockchain,
      project,
      version,
      block_month,
      block_date,
      block_time,
      trade_type,
      token_bought_symbol,
      token_sold_symbol,
      token_pair,
      token_bought_amount,
      token_sold_amount,
      token_bought_amount_raw,
      token_sold_amount_raw,
      amount_usd,
      token_bought_address,
      token_sold_address,
      taker,
      maker,
      project_contract_address,
      tx_hash,
      tx_from,
      tx_to,
      trace_address,
      evt_index
    from {{ dex_model }}
    {% if not loop.last %} union all {% endif %}
  {% endfor %}
) as combined_trades 
