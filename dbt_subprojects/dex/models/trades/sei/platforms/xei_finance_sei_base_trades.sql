{{
    config(
        schema = 'xei_finance_sei',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{{
    uniswap_compatible_v3_trades(
        blockchain = 'sei',
        project = 'xei_finance',
        version = '3',
        Pair_evt_Swap = source('xei_finance_sei', 'XeiV3Pool_evt_Swap'),
        Factory_evt_PoolCreated = source('xei_finance_sei', 'XeiV3Factory_evt_PoolCreated')
    )
}}