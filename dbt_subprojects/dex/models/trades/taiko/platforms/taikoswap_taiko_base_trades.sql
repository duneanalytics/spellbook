{{
    config(
        schema = 'taikoswap_taiko',
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
        blockchain = 'taiko',
        project = 'taikoswap',
        version = '1',
        Pair_evt_Swap = source('taikoswap_taiko', 'uniswapv2pair_evt_swap'),
        Factory_evt_PairCreated = source('taikoswap_taiko', 'uniswapv2factory_evt_paircreated')
    )
}}
