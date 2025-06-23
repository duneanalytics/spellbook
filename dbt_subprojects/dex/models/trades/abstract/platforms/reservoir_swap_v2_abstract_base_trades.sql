{{ config(
    schema = 'reservoir_swap_v2_abstract'
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
        blockchain = 'abstract'
        , project = 'reservoir_swap'
        , version = '1'
        , Pair_evt_Swap = source('reservoir_swap_abstract', 'uniswapv2pair_evt_swap')
        , Factory_evt_PoolCreated = source('reservoir_swap_abstract', 'uniswapv2factory_evt_paircreated')
    )
}}  