{{
    config(
        schema = 'silverswap_sonic',
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
        project = 'silverswap',
        version = '3',
        Pair_evt_Swap = source('silverswap_sonic', 'AlgebraPool_evt_Swap'),
        Factory_evt_PoolCreated = source('silverswap_sonic', 'AlgebraFactory_evt_Pool'),
        optional_columns = null
    )
}}