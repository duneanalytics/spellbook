{{
    config(
        schema = 'hyperjump_bnb',
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
        project = 'hyperjump',
        version = '1',
        Pair_evt_Swap = source('hyperjump_bnb', 'ThugswapPair_evt_Swap'),
        Factory_evt_PairCreated = source('hyperjump_bnb', 'ThugswapFactory_evt_PairCreated')
    )
}}
