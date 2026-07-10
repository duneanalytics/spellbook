{{ config(
    schema = 'uniswap_v4_polygon'
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
          blockchain = 'polygon'
        , project = 'uniswap'
        , version = '4'
        , PoolManager_evt_Initialize = source('uniswap_v4_polygon', 'PoolManager_evt_Initialize')
        , PoolManager_evt_Swap = source('uniswap_v4_polygon', 'PoolManager_evt_Swap') 
        , transactions = source('polygon', 'transactions')
        , monthly_relation = ref('uniswap_v4_polygon_sqrtpricex96_monthly')
    )
}}