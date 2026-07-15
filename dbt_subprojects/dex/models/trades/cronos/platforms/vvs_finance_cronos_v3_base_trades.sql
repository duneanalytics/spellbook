{{ config(
    schema = 'vvs_finance_v3_cronos'
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
        blockchain = 'cronos'
        , project = 'vvs_finance'
        , version = '3'
        , Pair_evt_Swap = source('vvsfinance_cronos', 'VVSV3Pool_evt_Swap')
        , Factory_evt_PoolCreated = source('vvsfinance_cronos', 'VVSV3Factory_evt_PoolCreated')
    )
}}
