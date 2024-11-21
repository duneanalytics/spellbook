{{
    config(
        schema = 'sushiswap_v1_bnb',
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
        project = 'sushiswap',
        version = '1',
        Pair_evt_Swap = source('sushi_bnb', 'UniswapV2Pair_evt_Swap'),
        Factory_evt_PairCreated = source('sushi_bnb', 'UniswapV2Factory_evt_PairCreated')
    )
}}
