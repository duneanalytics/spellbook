{{
    config(
        schema = 'somnex_v2_somnia',
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
        blockchain = 'somnia',
        project = 'somnex',
        version = '2',
        Pair_evt_Swap = source('somnex_somnia', 'somnexammpair_evt_swap'),
        Factory_evt_PairCreated = source('somnex_somnia', 'somnexammfactory_call_createpair'),
        pair_column_name = 'output_pair'
    )
}} 