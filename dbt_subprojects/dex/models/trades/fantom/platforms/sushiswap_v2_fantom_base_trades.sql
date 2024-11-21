{{
    config(
        schema = 'sushiswap_v2_fantom',
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
        blockchain = 'fantom',
        project = 'sushiswap',
        version = '2',
        Pair_evt_Swap = source('sushi_v2_fantom', 'Pool_evt_Swap'),
        Factory_evt_PoolCreated = source('sushi_v2_fantom', 'Factory_evt_PoolCreated')
    )
}}
