{{
    config(
        schema = 'echodex_linea',
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
        blockchain = 'linea',
        project = 'echodex',
        version = '1',
        Pair_evt_Swap = source('echodex_linea', 'EchodexPair_evt_Swap'),
        Factory_evt_PairCreated = source('echodex_linea', 'EchodexFactory_evt_PairCreated')
    )
}}
