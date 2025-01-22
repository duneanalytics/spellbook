{{
    config(
        schema = 'secta_linea',
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
        project = 'secta',
        version = '1',
        Pair_evt_Swap = source('secta_linea', 'SectaPair_evt_Swap'),
        Factory_evt_PairCreated = source('secta_linea', 'SectaFactory_evt_PairCreated')
    )
}}
