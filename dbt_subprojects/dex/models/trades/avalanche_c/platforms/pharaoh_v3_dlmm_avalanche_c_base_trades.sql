{{
    config(
        schema = 'pharaoh_v3_dlmm_avalanche_c',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{{
    trader_joe_compatible_v2_1_trades(
        blockchain = 'avalanche_c'
        , project = 'pharaoh_v3'
        , version = 'dlmm'
        , Pair_evt_Swap = source('pharaoh_v3_avalanche_c', 'dlmmpool_evt_swap')
        , Factory_evt_PoolCreated = source('pharaoh_v3_avalanche_c', 'dlmmfactory_evt_lbpaircreated')
        , pair_column_name = 'lbpair'
    )
}}
