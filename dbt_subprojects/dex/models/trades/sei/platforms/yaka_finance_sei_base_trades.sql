{{
    config(
        schema = 'yaka_sei',
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
        blockchain = 'sei',
        project = 'yaka',
        version = '2',
        Pair_evt_Swap = source('yaka_sei', 'Pair_evt_Swap'),
        Factory_evt_PairCreated = source('yaka_sei', 'PairFactory_evt_PairCreated')
    )
}}