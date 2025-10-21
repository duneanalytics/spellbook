{{ config(
    schema = 'pancakeswap_v3_ethereum'
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
          blockchain = 'ethereum'
        , project = 'pancakeswap'
        , version = '3'
        , token_transfers = source('erc20_ethereum', 'evt_Transfer')
        , liquidity_pools = ref('pancakeswap_ethereum_pools')
    )
}}

where block_time >= date '2025-10-01'