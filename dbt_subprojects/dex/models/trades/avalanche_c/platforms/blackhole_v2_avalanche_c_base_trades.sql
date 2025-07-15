{{
    config(
        schema = 'blackhole_v2_avalanche_c',
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
        project = 'blackhole',
        version = '2',
        Pair_evt_Swap = source('blackhole_avalanche_c', 'pair_evt_swap'),
        Factory_evt_PairCreated = source('blackhole_avalanche_c', 'pairgenerator_evt_paircreated')
    )
}}
