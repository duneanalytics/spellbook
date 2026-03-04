{{ config(
        schema = 'uniswap_monad',
        alias = 'base_liquidity_events'
        )
}}

{% set version_models = [
ref('uniswap_v3_monad_base_liquidity_events')
, ref('uniswap_v2_monad_base_liquidity_events')
] %}

WITH 

v2_v3_models as (

SELECT *
FROM (
    {% for dex_pool_model in version_models %}
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
    FROM {{ dex_pool_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
)
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
                , cast(null as int256) as liquidityDelta
                , cast(null as uint256) as sqrtPriceX96
                , cast(null as double) as tickLower
                , cast(null as double) as tickUpper
                , cast(null as varbinary) as salt 
        FROM 
        v2_v3_models 

        UNION ALL 

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
        {{ ref('uniswap_v4_monad_base_liquidity_events') }}

-- trigger refresh