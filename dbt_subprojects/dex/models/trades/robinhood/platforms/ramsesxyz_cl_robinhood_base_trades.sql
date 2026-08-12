{{
    config(
        schema = 'ramsesxyz_cl_robinhood',
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
        project = 'ramsesxyz',
        version = 'cl',
        Pair_evt_Swap = source('ramsesxyz_robinhood', 'RamsesV3Pool_evt_Swap'),
        Factory_evt_PoolCreated = source('ramsesxyz_robinhood', 'RamsesV3Factory_evt_PoolCreated'),
        optional_columns = []
    )
}}
