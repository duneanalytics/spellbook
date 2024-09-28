{{
    config(
        schema = 'solidly_v3_optimism',
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
        blockchain = 'optimism',
        project = 'solidly',
        version = '3',
        Pair_evt_Swap = source('solidly_v3_optimism', 'SolidlyV3Pool_evt_Swap'),
        Factory_evt_PoolCreated = source('solidly_v3_optimism', 'SolidlyV3Factory_evt_PoolCreated')
    )
}}
