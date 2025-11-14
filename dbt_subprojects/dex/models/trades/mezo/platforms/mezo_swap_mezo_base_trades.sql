{{
    config(
        schema = 'mezo_swap_mezo_base_trades',
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
        blockchain = 'mezo',
        project = 'mezo_swap',
        version = '1',
        Pair_evt_Swap = source('mezo_mezo', 'pool_evt_swap'),
        Factory_evt_PoolCreated = source('mezo_mezo', 'poolfactory_evt_poolcreated')
    )
}}
