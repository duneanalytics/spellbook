{{
    config(
        schema = 'sparkdex_v3_flare',
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
        blockchain = 'flare',
        project = 'sparkdex',
        version = '3',
        Pool_evt_Swap = source('sparkdex_flare', 'UniswapV3Pool_evt_Swap'),
        Factory_evt_PoolCreated = source('sparkdex_flare', 'UniswapV3Factory_evt_PoolCreated')
    )
}}
