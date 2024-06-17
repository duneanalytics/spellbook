{{
    config(
        schema = 'zebra_scroll',
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
        blockchain = 'scroll',
        project = 'zebra',
        version = '1',
        Pair_evt_Swap = source('zebra_scroll', 'ZebraV2Pool_evt_Swap'),
        Factory_evt_PoolCreated = source('zebra_scroll', 'ZebraV2Factory_evt_PoolCreated')
    )
}}
