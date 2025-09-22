{{ config(
    schema = 'velodrome_v1_optimism'
    , alias = 'base_liquidity_events'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['tx_hash', 'evt_index', 'event_type']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{{
    uniswap_compatible_v3_base_liquidity_events(
          blockchain = 'optimism'
        , project = 'velodrome'
        , version = '1'
        , token_transfers = source('erc20_base', 'evt_Transfer')
        , liquidity_pools = ref('velodrome_optimism_pools')
    )
}}