{{ config(
    schema = 'merchant_moe_v2_2_mantle'
    , alias = 'base_trades'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['tx_hash', 'evt_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{{
    trader_joe_compatible_v2_1_trades(
        blockchain = 'mantle'
        , project = 'merchant_moe'
        , version = '2.2'
        , Pair_evt_Swap = source('merchant_moe_v2_2_mantle', 'LBPair_evt_Swap')
        , Factory_evt_PoolCreated = source('merchant_moe_v2_2_mantle', 'LBFactory_evt_LBPairCreated')
    )
}}