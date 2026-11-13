{{ config(
    schema = 'uniswap_v3_blast'
    , alias = 'base_liquidity_events'
    , materialized = 'table'
    , file_format = 'delta'
    , tags=['static']
    , post_hook='{{ hide_spells() }}'
    )
}}

{{
    uniswap_compatible_v3_base_liquidity_events(
          blockchain = 'blast'
        , project = 'uniswap'
        , version = '3'
        , token_transfers = source('erc20_blast', 'evt_Transfer')
        , liquidity_pools = ref('uniswap_blast_pools')
    )
}}