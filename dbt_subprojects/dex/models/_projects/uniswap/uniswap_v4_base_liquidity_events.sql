{{ config(
        schema = 'uniswap_v4',
        alias = 'base_liquidity_events',
        post_hook='{{ expose_spells(blockchains = \'["bnb","ethereum","unichain","arbitrum"]\',
                                      spell_type = "project", 
                                      spell_name = "uniswap", 
                                      contributors = \'["Henrystats"]\') }}'
        )
}}

{% set models = [
    ref('uniswap_v4_arbitrum_base_liquidity_events')
   ,ref('uniswap_v4_avalanche_c_base_liquidity_events')
   ,ref('uniswap_v4_base_base_liquidity_events')
   ,ref('uniswap_v4_blast_base_liquidity_events')
   ,ref('uniswap_v4_bnb_base_liquidity_events')
   ,ref('uniswap_v4_ethereum_base_liquidity_events')
   ,ref('uniswap_v4_ink_base_liquidity_events')
   ,ref('uniswap_v4_optimism_base_liquidity_events')
   ,ref('uniswap_v4_polygon_base_liquidity_events')
   ,ref('uniswap_v4_unichain_base_liquidity_events')
   ,ref('uniswap_v4_worldchain_base_liquidity_events')
   ,ref('uniswap_v4_zora_base_liquidity_events')
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