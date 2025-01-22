{{
    config(
        schema = 'chronos_arbitrum',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{{
    uniswap_compatible_v2_trades(
        blockchain = 'arbitrum',
        project = 'chronos',
        version = '1',
        Pair_evt_Swap = source('chronos_arbitrum', 'PoolAMM_evt_Swap'),
        Factory_evt_PairCreated = source('chronos_arbitrum', 'PairFactoryAMM_evt_PairCreated')
    )
}}