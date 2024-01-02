{{
    config(
        schema = 'maverick_ethereum',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{{
    maverick_compatible_trades(
        blockchain = 'ethereum',
        project = 'maverick',
        version = '1',
        source_evt_swap = source('maverick_v1_ethereum', 'pool_evt_Swap'),
        source_evt_pool = source('maverick_v1_ethereum', 'factory_evt_PoolCreated')
    )
}}
