{{
    config(
        tags = ['prod_exclude'],
        schema = 'nuri_scroll',
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
        blockchain = 'scroll',
        project = 'nuri',
        version = '1',
        Pair_evt_Swap = source('nuri_scroll', 'ClPool_evt_Swap'),
        Factory_evt_PoolCreated = source('nuri_scroll', 'ClPoolFactory_evt_PoolCreated')
    )
}}