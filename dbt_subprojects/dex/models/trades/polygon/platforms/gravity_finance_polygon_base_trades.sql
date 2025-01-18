{{
    config(
        schema = 'gravity_finance_polygon',
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
        blockchain = 'polygon',
        project = 'gravity_finance',
        version = '1',
        Pair_evt_Swap = source('gravity_finance_polygon', 'UniswapV2Pair_evt_Swap'),
        Factory_evt_PairCreated = source('gravity_finance_polygon', 'UniswapV2Factory_evt_PairCreated')
    )
}}
