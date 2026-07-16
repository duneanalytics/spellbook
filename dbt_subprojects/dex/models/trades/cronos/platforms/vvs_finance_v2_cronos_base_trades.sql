{{ config(
    schema = 'vvs_finance_v2_cronos'
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
        blockchain = 'cronos'
        , project = 'vvs_finance'
        , version = '2'
        , Pair_evt_Swap = source('vvsfinance_cronos', 'VVSPair_evt_Swap')
        , Factory_evt_PairCreated = source('vvsfinance_cronos', 'VVSFactory_evt_PairCreated')
    )
}}
