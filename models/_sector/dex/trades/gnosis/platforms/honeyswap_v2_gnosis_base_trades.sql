{{
    config(
        schema = 'honeyswap_v2_gnosis',
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
        project = 'honeyswap',
        version = '2',
        Pair_evt_Swap = source('honeyswap_v2_gnosis', 'UniswapV2Pair_evt_Swap'),
        Factory_evt_PairCreated = source('honeyswap_v2_gnosis', 'UniswapV2Factory_evt_PairCreated')
    )
}}
