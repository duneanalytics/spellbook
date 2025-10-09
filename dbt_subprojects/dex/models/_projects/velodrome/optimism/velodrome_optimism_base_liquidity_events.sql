{{ config(
        schema = 'velodrome_optimism',
        alias = 'base_liquidity_events'
        )
}}

{% set version_models = [
ref('velodrome_v1_optimism_base_liquidity_events')
, ref('velodrome_v2_optimism_base_liquidity_events')
, ref('velodrome_2_cl_optimism_base_liquidity_events')
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
