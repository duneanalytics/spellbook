{{
    config(
        schema = 'izumi_finance_v3_flow',
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
        blockchain = 'flow',
        project = 'izumi_finance',
        version = '3',
        Pair_evt_Swap = source('izumi_finance_flow', 'iziswappool_evt_swap'),
        Factory_evt_PoolCreated = source('izumi_finance_flow', 'iziswapfactory_evt_newpool'),
        pair_column_name = 'pool'
    )
}} 