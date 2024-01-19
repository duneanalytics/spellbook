{{
    config(
        schema = 'zyberswap_arbitrum',
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
        project = 'zyberswap',
        version = '1',
        Pair_evt_Swap = source('zyberswap_arbitrum', 'ZyberPair_evt_Swap'),
        Factory_evt_PairCreated = source('zyberswap_arbitrum', 'ZyberFactory_evt_PairCreated')
    )
}}