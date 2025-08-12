{{ config(
    schema = 'eulerswap_unichain'
    , alias = 'base_trades'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['tx_hash', 'evt_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}


{{
    eulerswap_compatible_trades(
        blockchain = 'unichain'
        , project = 'eulerswap'
        , version = '1'
        , eulerswapinstance_evt_swap = source('eulerswap_unichain', 'eulerswapinstance_evt_swap')
        , eulerswap_pools_created = ref('eulerswap_unichain_pool_creations')
        , univ4_PoolManager_evt_Swap = source('uniswap_v4_unichain', 'PoolManager_evt_Swap') 
    )
}}
