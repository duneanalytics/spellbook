{{ config(
    schema = 'uniswap_v4_ethereum'
    , alias = 'sqrtpricex96'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['id', 'blockchain', 'block_index_sum']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{{
    uniswap_compatible_v4_liquidity_sqrtpricex96(
          blockchain = 'ethereum'
        , project = 'uniswap'
        , version = '4'
        , PoolManager_evt_Initialize = source('uniswap_v4_ethereum', 'PoolManager_evt_Initialize')
        , PoolManager_evt_Swap = source('uniswap_v4_ethereum', 'PoolManager_evt_Swap') 
    )
}}