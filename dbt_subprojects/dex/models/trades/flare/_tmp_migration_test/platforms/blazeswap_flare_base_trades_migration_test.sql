{{ config(
    schema = 'blazeswap_flare_migration_test'
    , alias = 'base_trades'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['tx_hash', 'evt_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    , tags = ['migration_test', 'prod_exclude']
    )
}}

{{
    uniswap_compatible_v2_trades(
        blockchain = 'flare'
        , project = 'blazeswap'
        , version = '2'
        , Pair_evt_Swap = source('blazeswap_flare', 'BLAZE_LP_evt_Swap')
        , Factory_evt_PairCreated = source('blazeswap_flare', 'BlazeSwapFactory_evt_PairCreated')
    )
}}
