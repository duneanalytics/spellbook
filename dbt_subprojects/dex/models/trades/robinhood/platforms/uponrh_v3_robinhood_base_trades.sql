{{
    config(
        schema = 'uponrh_v3_robinhood',
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
        blockchain = 'robinhood',
        project = 'uponrh',
        version = '3',
        Pair_evt_Swap = source('uponrh_robinhood', 'CLPool_evt_Swap'),
        Factory_evt_PoolCreated = source('uponrh_robinhood', 'CLFactory_evt_PoolCreated'),
        optional_columns = []
    )
}}
