{{
    config(
        schema = 'bladeswap_blast',
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
        blockchain = 'blast',
        project = 'bladeswap',
        version = '1',
        Pair_evt_Swap = source('bladeswap_blast', 'XYKPool_evt_Swap'),
        Factory_evt_PairCreated = source('bladeswap_blast', 'XYKPoolFactory_V2_evt_PoolCreated'),
        pair_column_name = 'pool'
    )
}}
