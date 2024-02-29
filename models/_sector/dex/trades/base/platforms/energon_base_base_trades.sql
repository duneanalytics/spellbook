{{
    config(
        schema = 'energon_base',
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
        project = 'energon',
        version = '1',
        Pair_evt_Swap = source('energon_base', 'EnergonDexPair_evt_Swap'),
        Factory_evt_PairCreated = source('energon_base', 'EnergonDexFactory_evt_PairCreated')
    )
}}