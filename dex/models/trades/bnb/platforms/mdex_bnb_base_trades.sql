{{
    config(
        schema = 'mdex_bnb',
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
        blockchain = 'bnb',
        project = 'mdex',
        version = '1',
        Pair_evt_Swap = source('mdex_bnb', 'MdexPair_evt_Swap'),
        Factory_evt_PairCreated = source('mdex_bnb', 'MdexFactory_evt_PairCreated')
    )
}}
