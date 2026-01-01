{{
    config(
        schema = 'flowswap_v3_flow',
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
        blockchain = 'flow',
        project = 'flowswap',
        version = '3',
        Pair_evt_Swap = source('flowswap_flow', 'uniswapv3pool_evt_Swap'),
        Factory_evt_PoolCreated = source('flowswap_flow', 'uniswapv3factory_evt_PoolCreated')
    )
}}