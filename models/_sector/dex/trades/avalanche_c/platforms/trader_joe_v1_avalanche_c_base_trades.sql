{{ config(
    schema = 'trader_joe_v1_avalanche_c'
    , alias = 'base_trades'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['tx_hash', 'evt_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{{
    uniswap_compatible_v2_trades(
        blockchain = 'avalanche_c'
        , project = 'trader_joe'
        , version = '1'
        , Pair_evt_Swap = source('trader_joe_avalanche_c', 'JoePair_evt_Swap')
        , Factory_evt_PairCreated = source('trader_joe_avalanche_c', 'JoeFactory_evt_PairCreated')
    )
}}