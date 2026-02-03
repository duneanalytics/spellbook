{{
    config(
        schema = 'kumbaya_megaeth',
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
        blockchain = 'megaeth',
        project = 'kumbaya',
        version = '1',
        Pair_evt_Swap = source('kumbaya_megaeth', 'v3pool_evt_swap'),
        Factory_evt_PoolCreated = source('kumbaya_megaeth', 'v3factory_evt_poolcreated')
    )
}}
