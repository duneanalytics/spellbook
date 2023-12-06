{{ config(
    schema = 'trader_joe_v2_avalanche_c'
    , alias = 'base_trades'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['tx_hash', 'evt_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{{
    trader_joe_compatible_v2_trades(
        blockchain = 'avalanche_c'
        , project = 'trader_joe'
        , version = '2'
        , Pair_evt_Swap = source('trader_joe_avalanche_c', 'LBPair_evt_Swap')
        , Factory_evt_PoolCreated = source('trader_joe_avalanche_c', 'LBFactory_evt_LBPairCreated')
    )
}}
