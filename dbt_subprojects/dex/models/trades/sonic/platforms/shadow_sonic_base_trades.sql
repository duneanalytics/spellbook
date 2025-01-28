{{
    config(
        schema = 'shadow_sonic',
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
        blockchain = 'sonic',
        project = 'shadow',
        version = '1',
        Pair_evt_Swap = source('shadow_sonic', 'Pair_evt_Swap'),
        Factory_evt_PairCreated = source('shadow_sonic', 'Core_PairFactory_evt_PairCreated')
    )
}}

{{
    uniswap_compatible_v3_trades(
        blockchain = 'sonic',
        project = 'shadow',
        version = '3',
        Pair_evt_Swap = source('shadow_sonic', 'RamsesV3Pool_evt_Swap'),
        Factory_evt_PoolCreated = source('shadow_sonic', 'RamsesV3Factory_evt_PoolCreated')
    )
}}