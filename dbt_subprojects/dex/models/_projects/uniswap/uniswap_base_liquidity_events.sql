{{ config(
    schema = 'uniswap'
    , alias = 'base_liquidity_events'
    , partition_by = ['block_month', 'blockchain', 'project']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'event_type']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{% set models = [
    ref('uniswap_v4_ethereum_base_liquidity_events')
   ,ref('uniswap_v4_arbitrum_base_liquidity_events')
   ,ref('uniswap_v4_optimism_base_liquidity_events')
   ,ref('uniswap_v4_blast_base_liquidity_events')
   ,ref('uniswap_v4_zora_base_liquidity_events')
   ,ref('uniswap_v4_avalanche_c_base_liquidity_events')
   ,ref('uniswap_v4_ink_base_liquidity_events')
   ,ref('uniswap_v4_unichain_base_liquidity_events')
   ,ref('uniswap_v4_base_base_liquidity_events')
   ,ref('uniswap_v4_polygon_base_liquidity_events')
   ,ref('uniswap_v4_bnb_base_liquidity_events')
   ,ref('uniswap_v4_worldchain_base_liquidity_events')
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
                , evt_index
                , event_type
                , token0
                , token1
                , amount0_raw
                , amount1_raw
        FROM
            {{ model }}
        {% if is_incremental() %}
        WHERE {{ incremental_predicate('block_time') }}
        {% endif %}
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
-- comment to refresh