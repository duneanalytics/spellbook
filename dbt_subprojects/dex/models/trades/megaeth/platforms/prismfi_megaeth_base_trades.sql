{{
    config(
        schema = 'prismfi_megaeth',
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
        project = 'prismfi',
        version = '1',
        Pair_evt_Swap = source('prism_megaeth', 'v3pool_evt_swap'),
        Factory_evt_PoolCreated = source('prism_megaeth', 'factory_evt_poolcreated')
    )
}}
