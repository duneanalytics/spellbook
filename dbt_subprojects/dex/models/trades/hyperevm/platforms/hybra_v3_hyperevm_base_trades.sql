{{
    config(
        schema = 'hybra_v3_hyperevm',
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
        blockchain = 'hyperevm',
        project = 'hybra',
        version = '3',
        Pair_evt_Swap = source('hybra_hyperevm', 'uniswapv3pool_evt_swap'),
        Factory_evt_PoolCreated = source('hybra_hyperevm', 'uniswapv3factory_evt_poolcreated')
    )
}} 