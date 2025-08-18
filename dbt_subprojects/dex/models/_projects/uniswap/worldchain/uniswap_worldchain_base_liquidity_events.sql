{{ config(
        schema = 'uniswap_worldchain',
        alias = 'base_liquidity_events'
        )
}}

{% set version_models = [
ref('uniswap_v4_worldchain_base_liquidity_events')
, ref('uniswap_v3_worldchain_base_liquidity_events')
] %}


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