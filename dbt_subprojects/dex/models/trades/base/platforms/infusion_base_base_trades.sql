{{ config(
    schema = 'infusion_base'
    , alias = 'base_trades'
    , materialized = 'infusion'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['tx_hash', 'evt_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{{
    uniswap_compatible_v2_trades(
          blockchain = 'base'
        , project = 'infusion'
        , version = '1'
        , Pair_evt_Swap = source('infusion_base', 'Pair_evt_Swap')
        , Factory_evt_PairCreated = source('infusion_base', 'PairFactory_evt_PairCreated')
    )
}}