{{ config(
    schema = 'throne_exchange_v2_base'
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
        , project = 'throne_exchange'
        , version = '2'
        , Pair_evt_Swap = source('throne_exchange_base', 'ThroneV2Pair_evt_Swap')
        , Factory_evt_PairCreated = source('throne_exchange_base', 'ThroneV2Factory_evt_PairCreated')
    )
}}