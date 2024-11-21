{{
    config(
        schema = 'elk_finance_avalanche_c',
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
        blockchain = 'avalanche_c',
        project = 'elk_finance',
        version = '1',
        Pair_evt_Swap = source('elk_finance_avalanche_c', 'ElkPair_evt_Swap'),
        Factory_evt_PairCreated = source('elk_finance_avalanche_c', 'ElkFactory_evt_PairCreated')
    )
}}
