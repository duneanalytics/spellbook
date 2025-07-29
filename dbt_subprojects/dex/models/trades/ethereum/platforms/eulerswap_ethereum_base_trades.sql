{{ config(
    schema = 'eulerswap_ethereum'
    , alias = 'base_trades'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['tx_hash', 'evt_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

with 

trades_cte as (
{{
    eulerswap_compatible_trades(
        blockchain = 'ethereum'
        , project = 'eulerswap'
        , version = '1'
        , eulerswapinstance_evt_swap = source('eulerswap_ethereum', 'eulerswapinstance_evt_swap')
        , eulerswap_pools_created = ref('eulerswap_ethereum_pools')
    )
}}
)

select * from trades_cte
where source != 'uni_v4' -- exclude trades logged in Uniswap V4 