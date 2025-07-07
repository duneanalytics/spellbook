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
   ,ref('dex_arbitrum_base_liquidity') 
   ,ref('dex_base_base_liquidity') 
   ,ref('dex_optimism_base_liquidity') 
   ,ref('dex_polygon_base_liquidity') 
   ,ref('dex_blast_base_liquidity') 
   ,ref('dex_bnb_base_liquidity') 
   ,ref('dex_zora_base_liquidity') 
   ,ref('dex_avalanche_c_base_liquidity') 
   ,ref('dex_ink_base_liquidity') 
   ,ref('dex_unichain_base_liquidity') 
   ,ref('dex_worldchain_base_liquidity') 
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
