{{ config(
    schema = 'uniswap_v4_robinhood'
    , alias = 'swaps'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['tx_hash', 'evt_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{{
    uniswap_compatible_v4_trades(
        blockchain = 'robinhood'
        , project = 'uniswap'
        , version = '4'
        , PoolManager_call_Swap = source('uniswap_v4_robinhood', 'PoolManager_call_Swap')
        , PoolManager_evt_Swap = source('uniswap_v4_robinhood', 'PoolManager_evt_Swap')
        , pool_manager_addr = '0x8366a39cc670b4001a1121b8f6a443a643e40951'
        , start_date = '2026-05-22'
    )
}}
