{{ config(
    schema = 'uniswap_v4_blast'
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
          blockchain = 'blast'
        , project = 'uniswap'
        , version = '4'
        , PoolManager_evt_ModifyLiquidity = source ('uniswap_v4_blast', 'PoolManager_evt_ModifyLiquidity')
        , PoolManager_evt_Swap = source('uniswap_v4_blast', 'PoolManager_evt_Swap') 
        , liquidity_pools = ref('uniswap_v4_blast_pools')
        , liquidity_sqrtpricex96 = ref('uniswap_v4_blast_sqrtpricex96')
        , PoolManager_call_Take = source('uniswap_v4_blast', 'poolmanager_call_take')
    )
}}