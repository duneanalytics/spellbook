{{
    config(
        schema = 'quickswap_v3_somnia',
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
        blockchain = 'somnia',
        project = 'quickswap',
        version = '3',
        Pair_evt_Swap = source('quickswap_somnia', 'algebrapool_evt_swap'),
        Factory_evt_PoolCreated = source('quickswap_somnia', 'algebrafactory_call_createpool'),
        pair_column_name = 'output_0'
    )
}} 