{{ config(
    schema = 'uniswap_v4_tempo'
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
        blockchain = 'tempo'
        , project = 'uniswap'
        , version = '4'
        , PoolManager_call_Swap = source('uniswap_v4_tempo', 'PoolManager_call_Swap')
        , PoolManager_evt_Swap = source('uniswap_v4_tempo', 'PoolManager_evt_Swap')
        , pool_manager_addr = '0x33620f62c5b9b2086dd6b62f4a297a9f30347029'
        , start_date = '2026-02-26'
    )
}}
