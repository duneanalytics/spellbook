{{
    config(
        schema = 'dragon_swap_v2_kaia',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{{
    uniswap_compatible_v2_trades(
        blockchain = 'kaia',
        project = 'dragon_swap',
        version = '2',
        Pair_evt_Swap = source('dragonswap_kaia', 'PancakePair_evt_Swap'),
        Factory_evt_PairCreated = source('dragonswap_kaia', 'PancakeV2Factory_evt_PairCreated')
    )
}}
