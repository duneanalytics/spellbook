{{
    config(
        schema = 'sparkdex_v2_flare',
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
        blockchain = 'flare',
        project = 'sparkdex',
        version = '2',
        Pair_evt_Swap = source('sparkdex_flare', 'UniswapV2Pair_evt_Swap'),
        Factory_evt_PairCreated = source('sparkdex_flare', 'UniswapV2Factory_evt_PairCreated')
    )
}}
