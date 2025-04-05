{{
    config(
        schema = 'klay_swap_v3_kaia',
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
        project = 'klay_swap',
        version = '3',
        Pair_evt_Swap = source('klay_swap_v3_kaia', 'Pool_evt_Swap'),
        Factory_evt_PoolCreated = source('klay_swap_v3_kaia', 'Factory_evt_PoolCreated')
    )
}}
