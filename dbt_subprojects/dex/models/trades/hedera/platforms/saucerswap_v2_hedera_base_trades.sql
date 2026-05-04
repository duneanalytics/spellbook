{{
    config(
        schema = 'saucerswap_v2_hedera',
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
        blockchain = 'hedera',
        project = 'saucerswap',
        version = '2',
        Pair_evt_Swap = source('saucer_swap_hedera', 'v2pool_evt_swap'),
        Factory_evt_PoolCreated = source('saucer_swap_hedera', 'saucerswapv2factory_evt_poolcreated')
    )
}}
