{{
    config(
        schema = 'squidswap_ink',
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
        blockchain = 'ink',
        project = 'squidswap',
        version = '1',
        Pair_evt_Swap = source('squidswap_ink', 'squidswapv2pair_evt_swap'),
        Factory_evt_PairCreated = source('squidswap_ink', 'squidswapv2factory_evt_paircreated')
    )
}}
