{{ config(
    schema = 'uniswap_v4_bnb'
    , alias = 'bnb_liquidity'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['tx_hash', 'evt_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{{
    uniswap_compatible_v4_liquidity(
          blockchain = 'bnb'
        , project = 'uniswap'
        , version = '4'
        , PoolManager_evt_ModifyLiquidity = source ('uniswap_v4_bnb', 'PoolManager_evt_ModifyLiquidity')
        , PoolManager_evt_Swap = source('uniswap_v4_bnb', 'PoolManager_evt_Swap') 
        , PoolManager_call_Swap = source('uniswap_v4_bnb', 'PoolManager_call_Swap') 
        , PoolManager_evt_Initialize = source('uniswap_v4_bnb', 'PoolManager_evt_Initialize')
    )
}}