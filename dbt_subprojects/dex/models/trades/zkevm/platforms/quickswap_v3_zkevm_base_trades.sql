{{
    config(
        schema = 'quickswap_v3_zkevm',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}


{{
    uniswap_compatible_v3_trades(
        blockchain = 'zkevm'
        , project = 'quickswap'
        , version = '3'
        , Pair_evt_Swap = source('quickswap_zkevm', 'algebra_pool_zkevm_evt_Swap')
        , Factory_evt_PoolCreated = source('quickswap_zkevm', 'algebra_factory_zkevm_evt_Pool')
        , optional_columns = null
    )
}}
