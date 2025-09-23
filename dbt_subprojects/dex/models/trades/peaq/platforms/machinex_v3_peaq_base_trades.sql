{{
    config(
        schema = 'machinex_v3_peaq',
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
        blockchain = 'peaq',
        project = 'machinex',
        version = '3',
        Pair_evt_Swap = source('machinex_peaq', 'machinexv3pool_evt_swap'),
        Factory_evt_PoolCreated = source('machinex_peaq', 'machinexv3factory_call_createpool'),
        pair_column_name = 'output_0'
    )
}} 