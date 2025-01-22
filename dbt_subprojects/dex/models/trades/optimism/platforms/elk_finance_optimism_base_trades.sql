{{
    config(
        schema = 'elk_finance_optimism',
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
        blockchain = 'optimism',
        project = 'elk_finance',
        version = '1',
        Pair_evt_Swap = source('elk_finance_optimism', 'ElkPair_evt_Swap'),
        Factory_evt_PairCreated = source('elk_finance_optimism', 'ElkFactory_evt_PairCreated')
    )
}}
