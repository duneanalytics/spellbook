{{ config(
    schema = 'pancakeswap_infinity_cl'
    , alias = 'base_liquidity_events'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['tx_hash', 'evt_index', 'event_type']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{{
    pancakeswap_compatible_infinity_base_liquidity_events(
          blockchain = 'bnb'
        , project = 'pancakeswap'
        , version = 'infinity_cl'
        , PoolManager_evt_ModifyLiquidity = source ('pancakeswap_infinity_bnb', 'clpoolmanager_evt_modifyliquidity')
        , PoolManager_evt_Swap = source('pancakeswap_infinity_bnb', 'ClPoolManager_evt_Swap') 
        , liquidity_pools = ref('pancakeswap_infinity_cl_bnb_pools')
        , liquidity_sqrtpricex96 = ref('pancakeswap_infinity_cl_bnb_sqrtpricex96')
    )
}}