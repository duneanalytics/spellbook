{{
    config(
        schema = 'sharkyswap_arbitrum',
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
        project = 'sharkyswap',
        version = '1',
        Pair_evt_Swap = source('sharkyswap_arbitrum', 'SharkyPair_evt_Swap'),
        Factory_evt_PairCreated = source('sharkyswap_arbitrum', 'SharkyFactory_evt_PairCreated')
    )
}}
