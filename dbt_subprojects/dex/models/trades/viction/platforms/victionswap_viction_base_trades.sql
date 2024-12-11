{{
    config(
        schema = 'victionswap_viction',
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
        blockchain = 'viction',
        project = 'victionswap',
        version = '2',
        Pair_evt_Swap = source('victionswap_viction', 'UniswapV2Pair_evt_Swap'),
        Factory_evt_PairCreated = source('victionswap_viction', 'UniswapV2Factory_evt_PairCreated')
    )
}}
