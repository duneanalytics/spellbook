{{
    config(
        schema = 'saucerswap_v1_hedera',
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
        blockchain = 'hedera',
        project = 'saucerswap',
        version = '1',
        Pair_evt_Swap = source('saucer_swap_hedera', 'pool_evt_swap'),
        Factory_evt_PairCreated = source('saucer_swap_hedera', 'v1_factory_evt_paircreated')
    )
}}
