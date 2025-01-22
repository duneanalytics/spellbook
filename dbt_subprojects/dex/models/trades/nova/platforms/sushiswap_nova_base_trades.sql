{{
    config(
        schema = 'sushiswap_nova',
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
        blockchain = 'nova',
        project = 'sushiswap',
        version = '1',
        Pair_evt_Swap = source('sushi_nova', 'uniswapv2pair_evt_swap'),
        Factory_evt_PairCreated = source('sushi_nova', 'uniswapv2factory_evt_paircreated')
    )
}}
