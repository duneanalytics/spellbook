{{
    config(
        schema = 'camelot_v3_arbitrum',
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
        blockchain = 'arbitrum',
        project = 'camelot',
        version = '3',
        Pair_evt_Swap = source('camelot_v3_arbitrum', 'AlgebraPool_evt_Swap'),
        Factory_evt_PoolCreated = source('camelot_v3_arbitrum', 'AlgebraFactory_evt_Pool'),
        optional_columns = []
    )
}}
