{{
    config(
        schema = 'bulletx_v3_superseed',
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
        blockchain = 'superseed',
        project = 'bulletx',
        version = '3',
        Pair_evt_Swap = source('bulletx_superseed', 'V3Pool_evt_Swap'),
        Factory_evt_PoolCreated = source('bulletx_superseed', 'BulletXV3Factory_evt_PoolCreated')
    )
}} 