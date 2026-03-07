{{
    config(
        schema = 'pinot_v3_monad_base_trades',
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
        blockchain = 'monad',
        project = 'pinot',
        version = '3',
        Pair_evt_Swap = source('pinot_monad', 'uniswapv3pool_evt_swap'),
        Factory_evt_PoolCreated = source('pinot_monad', 'uniswapv3factory_evt_poolcreated')
    )
}}
