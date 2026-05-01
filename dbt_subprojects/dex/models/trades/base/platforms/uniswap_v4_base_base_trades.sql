{{ config(
    schema = 'uniswap_v4_base'
    , alias = 'base_trades'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['tx_hash', 'evt_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{{
    uniswap_compatible_v4_trades(
        blockchain = 'base'
        , project = 'uniswap'
        , version = '4'
        , PoolManager_call_Swap = source('uniswap_v4_base', 'PoolManager_call_Swap') 
        , PoolManager_evt_Swap = source('uniswap_v4_base', 'PoolManager_evt_Swap') 
        , pool_manager_addr = '0x498581ff718922c3f8e6a244956af099b2652b2b'
        , start_date = '2025-01-23'
    )
}}