{{
    config(
        schema = 'scale_base',
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
        blockchain = 'base',
        project = 'scale',
        version = '1',
        Pair_evt_Swap = source('scale_base', 'Pair_evt_Swap'),
        Factory_evt_PairCreated = source('scale_base', 'PairFactory_evt_PairCreated')
    )
}}