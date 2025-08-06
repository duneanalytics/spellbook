{{
    config(
        schema = 'unagi_v3_taiko',
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
        blockchain = 'taiko',
        project = 'unagi',
        version = '3',
        Pair_evt_Swap = source('unagi_taiko', 'v3pool_evt_swap'),
        Factory_evt_PoolCreated = source('unagi_taiko', 'v3factory_evt_poolcreated')
    )
}}
