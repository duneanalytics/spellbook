{{
    config(
        schema = 'sushiswap_v1_gnosis',
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
        blockchain = 'gnosis',
        project = 'sushiswap',
        version = '1',
        Pair_evt_Swap = source('sushiswap_gnosis', 'UniswapV2Pair_evt_Swap'),
        Factory_evt_PairCreated = source('sushiswap_gnosis', 'UniswapV2Factory_evt_PairCreated')
    )
}}
