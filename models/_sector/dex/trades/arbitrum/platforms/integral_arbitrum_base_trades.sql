{{
    config(
        schema = 'integral_arbitrum',
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
        blockchain = 'arbitrum',
        project = 'integral',
        version = 'size',
        Pair_evt_Swap = source('integral_size_arbitrum', 'TwapPair_evt_Swap'),
        Factory_evt_PairCreated = source('integral_size_arbitrum', 'TwapFactory_evt_PairCreated')
    )
}}
