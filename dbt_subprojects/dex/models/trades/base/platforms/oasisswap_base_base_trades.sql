{{
    config(
        schema = 'oasisswap_base',
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
        project = 'oasisswap',
        version = '1',
        Pair_evt_Swap = source('oasisswap_base', 'OasisSwapPair_evt_Swap'),
        Factory_evt_PairCreated = source('oasisswap_base', 'OasisSwapFactory_evt_PairCreated')
    )
}}
