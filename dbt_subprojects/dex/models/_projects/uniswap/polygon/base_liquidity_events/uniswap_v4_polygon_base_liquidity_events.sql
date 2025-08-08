{{ config(
    schema = 'uniswap_v4_polygon'
    , alias = 'base_liquidity_events'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['tx_hash', 'evt_index', 'event_type']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{{
    uniswap_compatible_v4_base_liquidity_events(
          blockchain = 'polygon'
        , project = 'uniswap'
        , version = '4'
        , PoolManager_evt_ModifyLiquidity = source ('uniswap_v4_polygon', 'PoolManager_evt_ModifyLiquidity')
        , PoolManager_evt_Swap = source('uniswap_v4_polygon', 'PoolManager_evt_Swap') 
        , liquidity_pools = ref('uniswap_v4_polygon_pools')
        , liquidity_sqrtpricex96 = ref('uniswap_v4_polygon_sqrtpricex96')
        , PoolManager_call_Take = source('uniswap_v4_polygon', 'poolmanager_call_take')
    )
}}