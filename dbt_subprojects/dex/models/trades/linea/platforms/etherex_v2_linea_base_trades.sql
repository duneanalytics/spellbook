{{
    config(
        schema = 'etherex_v2_linea',
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
        project = 'etherex',
        version = '2',
        Pair_evt_Swap = source('etherex_linea', 'Pair_evt_Swap'),
        Factory_evt_PairCreated = source('etherex_linea', 'PairFactory_evt_PairCreated')
    )
}}
