{{ config(
    schema = 'uniswap_v2_arbitrum'
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
          blockchain = 'arbitrum'
        , project = 'uniswap'
        , version = '2'
        , token_transfers = source('erc20_arbitrum', 'evt_Transfer')
        , liquidity_pools = ref('uniswap_arbitrum_pools')
    )
}}