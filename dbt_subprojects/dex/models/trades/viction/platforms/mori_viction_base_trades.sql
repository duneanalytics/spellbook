{{
    config(
        schema = 'mori_viction',
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
        blockchain = 'viction',
        project = 'mori',
        version = '3',
        Pair_evt_Swap = source('mori_viction', 'UniswapV3Pool_evt_Swap'),
        Factory_evt_PoolCreated = source('mori_viction', 'UniswapV3Factory_evt_PoolCreated')
    )
}}
