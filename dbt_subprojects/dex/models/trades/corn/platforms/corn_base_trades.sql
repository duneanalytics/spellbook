{{
    config(
        schema = 'oku_corn',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{{
    uniswap_compatible_v3_trades(
        blockchain = 'corn',
        project = 'oku',
        version = '3',
        Pair_evt_Swap = source('oku_corn', 'UniswapV3Pool_evt_Swap'),
        Factory_evt_PoolCreated = source('oku_corn', 'UniswapV3Factory_evt_PoolCreated')
    )
}}
