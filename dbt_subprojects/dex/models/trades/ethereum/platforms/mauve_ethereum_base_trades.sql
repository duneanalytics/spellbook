{{
    config(
        schema = 'mauve_ethereum',
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
        blockchain = 'ethereum',
        project = 'mauve',
        version = '1',
        Pair_evt_Swap = source('mauve_ethereum', 'MauvePool_evt_Swap'),
        Factory_evt_PoolCreated = source('mauve_ethereum', 'MauveFactory_evt_PoolCreated')
    )
}}
