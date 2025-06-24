{{
    config(
        schema = 'swapx_sonic',
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
        blockchain = 'sonic',
        project = 'swapx',
        version = '1',
        Pair_evt_Swap = source('swapx_sonic', 'AlgebraPool_evt_Swap'),
        Factory_evt_PoolCreated = source('swapx_sonic', 'AlgebraFactory_evt_Pool'),
        
        )
}}