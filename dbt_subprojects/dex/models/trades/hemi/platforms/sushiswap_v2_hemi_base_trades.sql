{{
    config(
        schema = 'sushiswap_v2_hemi',
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
        blockchain = 'hemi',
        project = 'sushiswap',
        version = '2',
        Pair_evt_Swap = source('sushiswap_hemi', 'v2pair_evt_swap'),
        Factory_evt_PairCreated = source('sushiswap_hemi', 'v2factory_evt_paircreated')
    )
}}
