{{
    config(
        schema = 'scrollswap_scroll',
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
        blockchain = 'scroll',
        project = 'scrollswap',
        version = '1',
        Pair_evt_Swap = source('scrollswap_scroll', 'Pair_evt_Swap'),
        Factory_evt_PairCreated = source('scrollswap_scroll', 'Factory_evt_PairCreated')
    )
}}
