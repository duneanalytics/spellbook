{{ config(
    schema = 'citadelswap_base'
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
          blockchain = 'base'
        , project = 'citadelswap'
        , version = '1'
        , Pair_evt_Swap = source('citadelswap_base', 'CitadelPair_evt_Swap')
        , Factory_evt_PairCreated = source('citadelswap_base', 'CitadelFactory_evt_PairCreated')
    )
}}