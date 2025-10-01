{{ config(
    schema = 'ekubo',
    alias = 'base_liquidity_events',
    materialized = 'view'
    )
}}

{% set models = [
    ref('ekubo_ethereum_base_liquidity_events')
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