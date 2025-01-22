{{
    config(
        schema = 'dragon_swap_v3_kaia',
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
        blockchain = 'kaia',
        project = 'dragon_swap',
        version = '3',
        Pair_evt_Swap = source('dragonswap_kaia', 'PancakeV3Pool_evt_Swap'),
        Factory_evt_PoolCreated = source('dragonswap_kaia', 'PancakeV3Factory_evt_PoolCreated')
    )
}}
