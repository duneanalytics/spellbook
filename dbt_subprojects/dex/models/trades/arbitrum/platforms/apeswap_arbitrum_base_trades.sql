{{
    config(
        schema = 'apeswap_arbitrum',
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
        project = 'apeswap',
        version = '2',
        Pair_evt_Swap = source('apeswap_arbitrum', 'ApePair_evt_Swap'),
        Factory_evt_PairCreated = source('apeswap_arbitrum', 'ApeFactory_evt_PairCreated')
    )
}}
