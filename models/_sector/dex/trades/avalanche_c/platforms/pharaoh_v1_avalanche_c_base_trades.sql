{{ config(
    schema = 'pharaoh_v1_avalanche_c_base_trades'
    , alias = 'base_trades'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['tx_hash', 'evt_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{{
    uniswap_compatible_v3_trades(
        blockchain = 'avalanche_c'
        , project = 'pharaoh'
        , version = '1'
        , Pair_evt_Swap = source('pharaoh_v1_avalanche_c', 'ClPool_evt_Swap')
        , Factory_evt_PoolCreated = source('pharaoh_v1_avalanche_c', 'ClPoolFactory_evt_PoolCreated')
    )
}}
