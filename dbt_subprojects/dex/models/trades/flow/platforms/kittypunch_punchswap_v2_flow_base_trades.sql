{{
    config(
        schema = 'kittypunch_punchswap_v2_flow',
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
        blockchain = 'flow',
        project = 'kittypunch',
        version = '2',
        Pair_evt_Swap = source('kittypunch_flow', 'punchswapv2pair_evt_Swap'),
        Factory_evt_PairCreated = source('kittypunch_flow', 'punchswapv2factory_evt_paircreated')
    )
}}