{{ config(
    schema = 'trader_joe_v2_arbitrum'
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
        blockchain = 'arbitrum'
        , project = 'trader_joe'
        , version = '2'
        , Pair_evt_Swap = source('trader_joe_arbitrum', 'LBPair_evt_Swap')
        , Factory_evt_PoolCreated = source('trader_joe_arbitrum', 'LBFactory_evt_LBPairCreated')
    )
}}
