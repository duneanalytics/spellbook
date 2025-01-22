{{
    config(
        schema = 'maverick_v2_ethereum',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{{
    maverick_compatible_v2_trades(
        blockchain = 'ethereum',
        project = 'maverick',
        version = '2',
        source_evt_swap = source('maverick_v2_ethereum', 'V2Pool_evt_PoolSwap'),
        source_evt_pool = source('maverick_v2_ethereum', 'V2Factory_evt_PoolCreated')
    )
}}
