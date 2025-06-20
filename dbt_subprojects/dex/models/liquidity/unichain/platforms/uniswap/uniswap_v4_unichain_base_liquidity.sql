{{ config(
    schema = 'uniswap_v4_unichain'
    , alias = 'base_liquidity'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['tx_hash', 'evt_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{{
    uniswap_compatible_v4_base_liquidity(
          blockchain = 'unichain'
        , project = 'uniswap'
        , version = '4'
        , PoolManager_evt_ModifyLiquidity = source ('uniswap_v4_unichain', 'PoolManager_evt_ModifyLiquidity')
        , PoolManager_evt_Swap = source('uniswap_v4_unichain', 'PoolManager_evt_Swap') 
        , liquidity_pools = ref('uniswap_v4_unichain_pools')
        , liquidity_sqrtpricex96 = ref('uniswap_v4_unichain_sqrtpricex96')
        , liquidity_traces = source('unichain', 'traces')
    )
}}