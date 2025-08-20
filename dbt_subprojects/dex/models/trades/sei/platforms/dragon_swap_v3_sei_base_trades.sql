{{
    config(
        schema = 'dragon_swap_v3_sei',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

SELECT DISTINCT *
FROM {{
    uniswap_compatible_v3_trades(
        blockchain = 'sei',
        project = 'dragon_swap',
        version = '3',
        Pair_evt_Swap = source('dragon_swap_sei_v2_sei', 'dragonswapv2pool_evt_swap'),
        Factory_evt_PoolCreated = source('dragon_swap_sei_v2_sei', 'dragonswapv2factory_evt_poolcreated')
    )
}}