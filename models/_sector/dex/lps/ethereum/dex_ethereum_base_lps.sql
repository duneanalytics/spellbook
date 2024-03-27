{{ config(
    schema = 'dex_ethereum'
    , alias = 'base_lps'
    , materialized = 'view'
    )
}}

{% set base_models = [
    ref('uniswap_v3_ethereum_base_lps')
] %}

WITH base_union AS (
    SELECT *
    FROM (
        {% for base_model in base_models %}
        SELECT
            blockchain
            , project
            , version
            , block_time
            , block_month
            , block_number
            , amount0_raw
            , amount1_raw
            , liquidity_raw
            , token0_address
            , token1_address
            , pool_address
            , liquidity_provider
            , position_id
            , tick_lower
            , tick_upper
            , tx_hash
            , evt_index
            , row_number() over (partition by tx_hash, evt_index order by tx_hash) as duplicates_rank
        FROM 
            {{ base_model }}
        {% if is_incremental() %}
        WHERE
            {{ incremental_predicate('block_time') }}
        {% endif %}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
    )
    WHERE
        duplicates_rank = 1
)

select
    *
from
    base_union