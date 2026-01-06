{{
    config(
        schema = 'kittypunch_punchswap_v3_flow',
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
        project = 'kittypunch',
        version = '3',
        Pair_evt_Swap = source('kittypunch_flow', 'punchswapv3pool_evt_swap'),
        Factory_evt_PoolCreated = source('kittypunch_flow', 'punchswapv3factory_evt_poolcreated')
    )
}}