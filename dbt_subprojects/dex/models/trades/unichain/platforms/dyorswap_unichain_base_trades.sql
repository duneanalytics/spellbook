{{
    config(
        schema = 'dyorswap_unichain',
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
        blockchain = 'unichain',
        project = 'dyorswap',
        version = '2',
        Pair_evt_Swap = source('dyorswap_unichain', 'DyorswapV2Pair_evt_Swap'),
        Factory_evt_PairCreated = source('dyorswap_unichain', 'DyorswapV2Factory_evt_PairCreated')
    )
}} 