{{ config(
    schema = 'dex'
    , alias = 'base_liquidity'
    , partition_by = ['block_month', 'blockchain', 'project']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['blockchain', 'project', 'version', 'tx_hash', 'evt_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{% set models = [
    ref('dex_ethereum_base_liquidity')
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
                , salt
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
