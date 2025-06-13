{{
    config(
        schema = 'mindgames_arbitrum',
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
        blockchain = 'arbitrum',
        project = 'mindgames',
        version = '1',
        Pair_evt_Swap = source('mindgames_arbitrum', 'lp_crx_gmx_evt_swap'),
        Factory_evt_PairCreated = source('mindgames_arbitrum', 'uniswapv2factory_evt_paircreated')
    )
}}