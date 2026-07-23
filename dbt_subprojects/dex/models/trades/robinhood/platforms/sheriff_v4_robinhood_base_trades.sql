{{
    config(
        schema = 'sheriff_v4_robinhood',
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
        project = 'sheriff',
        version = '4',
        Pair_evt_Swap = source('sheriff_robinhood', 'AlgebraPool_evt_Swap'),
        Factory_evt_PoolCreated = source('sheriff_robinhood', 'AlgebraFactory_evt_Pool'),
        optional_columns = []
    )
}}
