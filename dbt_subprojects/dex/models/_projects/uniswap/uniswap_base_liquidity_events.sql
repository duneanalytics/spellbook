{{ config(
        schema = 'uniswap',
        alias = 'base_liquidity_events'
        )
}}

{% set models = [
    ref('uniswap_arbitrum_base_liquidity_events')
   ,ref('uniswap_avalanche_c_base_liquidity_events')
   ,ref('uniswap_base_base_liquidity_events')
   ,ref('uniswap_blast_base_liquidity_events')
   ,ref('uniswap_bnb_base_liquidity_events')
   ,ref('uniswap_celo_base_liquidity_events')
   ,ref('uniswap_ethereum_base_liquidity_events')
   ,ref('uniswap_gnosis_base_liquidity_events')
   ,ref('uniswap_ink_base_liquidity_events')
   ,ref('uniswap_linea_base_liquidity_events')
   ,ref('uniswap_mantle_base_liquidity_events')
   ,ref('uniswap_monad_base_liquidity_events')
   ,ref('uniswap_optimism_base_liquidity_events')
   ,ref('uniswap_plasma_base_liquidity_events')
   ,ref('uniswap_polygon_base_liquidity_events')
   ,ref('uniswap_scroll_base_liquidity_events')
   ,ref('uniswap_sonic_base_liquidity_events')
   ,ref('uniswap_unichain_base_liquidity_events')
   ,ref('uniswap_worldchain_base_liquidity_events')
   ,ref('uniswap_zksync_base_liquidity_events')
   ,ref('uniswap_zora_base_liquidity_events')
] %}

with base_union as (
    SELECT *
    FROM
    (
        {% for model in models %}
        SELECT
                 blockchain
                , project
                , version
                , block_month
                , block_date
                , block_time
                , block_number
                , id
                , tx_hash
                , tx_from
                , evt_index
                , event_type
                , token0
                , token1
                , amount0_raw
                , amount1_raw
                , liquidityDelta
                , sqrtPriceX96
                , tickLower
                , tickUpper
                , salt
        FROM
            {{ model }}
        {% if not loop.last %}
           UNION ALL
        {% endif %}
        {% endfor %}
    )
)
select
    *
from
    base_union

