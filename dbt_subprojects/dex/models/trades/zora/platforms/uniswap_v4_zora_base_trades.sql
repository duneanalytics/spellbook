{{ config(
    schema = 'uniswap_v4_zora'
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
        blockchain = 'zora'
        , project = 'uniswap'
        , version = '4'
        , PoolManager_evt_Swap = source('uniswap_v4_zora', 'PoolManager_evt_Swap') 
        , PoolManager_evt_Initialize = source('uniswap_v4_zora', 'PoolManager_evt_Initialize')
        , taker_column_name = 'evt_tx_from'
    )
}}