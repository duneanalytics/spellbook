{{
    config(
        schema = 'nile_linea',
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
        blockchain = 'linea',
        project = 'nile',
        version = '1',
        Pair_evt_Swap = source('nile_linea', 'ClPool_evt_Swap'),
        Factory_evt_PoolCreated = source('nile_linea', 'ClPoolFactory_evt_PoolCreated')
    )
}}
