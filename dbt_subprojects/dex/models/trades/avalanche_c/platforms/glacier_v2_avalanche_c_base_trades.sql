{{
    config(
        schema = 'glacier_v2_avalanche_c',
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
        blockchain = 'avalanche_c',
        project = 'glacier',
        version = '2',
        Pair_evt_Swap = source('glacier_avalanche_c', 'Pair_evt_Swap'),
        Factory_evt_PairCreated = source('glacier_avalanche_c', 'PairFactory_evt_PairCreated')
    )
}}
