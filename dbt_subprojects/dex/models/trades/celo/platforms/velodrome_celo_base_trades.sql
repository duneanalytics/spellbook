{{
    config(
        schema = 'velodrome_celo',
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
        blockchain = 'celo',
        project = 'velodrome',
        version = '2_cl',
        Pair_evt_Swap = source('velodrome_v2_celo', 'clpool_evt_swap'),
        Factory_evt_PoolCreated = source('velodrome_v2_celo', 'poolfactory_evt_poolcreated'),
        optional_columns = []
    )
}}
