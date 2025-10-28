{{ config(
    schema = 'pancakeswap_infinity_cl_base'
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
          blockchain = 'base'
        , project = 'pancakeswap'
        , version = 'infinity_cl'
        , PoolManager_evt_Initialize = source('pancakeswap_infinity_base', 'clpoolmanager_evt_initialize')
        , PoolManager_evt_Swap = source('pancakeswap_infinity_base', 'ClPoolManager_evt_Swap') 
    )
}}