{{ config(
    schema = 'uniswap_v4_xlayer'
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
        blockchain = 'xlayer'
        , project = 'uniswap'
        , version = '4'
        , PoolManager_call_Swap = source('uniswap_v4_xlayer', 'PoolManager_call_Swap')
        , PoolManager_evt_Swap = source('uniswap_v4_xlayer', 'PoolManager_evt_Swap')
        , pool_manager_addr = '0x360e68faccca8ca495c1b759fd9eee466db9fb32'
        , start_date = '2025-12-08'
        , aggregator_hooks = ref('uniswap_v4_xlayer_aggregator_hooks')
    )
}}
