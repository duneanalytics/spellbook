{{
    config(
        schema = 'bulletx_superseed',
        alias = 'v2_base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{{
    uniswap_compatible_v2_trades(
        blockchain = 'superseed',
        project = 'bulletx',
        version = '2',
        Pair_evt_Swap = source('bulletx_superseed', 'V2Pair_evt_Swap'),
        Factory_evt_PairCreated = source('bulletx_superseed', 'BulletXFactory_call_createPair')
    )
}} 