{{
    config(
        schema = 'zipswap_optimism',
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
        blockchain = 'optimism',
        project = 'zipswap',
        version = '1',
        Pair_evt_Swap = source('zipswap_optimism', 'UniswapV2Pair_evt_Swap'),
        Factory_evt_PairCreated = source('zipswap_optimism', 'UniswapV2Factory_evt_PairCreated')
    )
}}
