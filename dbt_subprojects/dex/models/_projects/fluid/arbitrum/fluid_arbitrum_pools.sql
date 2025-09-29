{{
    config(
        schema = 'fluid_arbitrum',
        alias = 'pools',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['dex'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{{
    fluid_liquidity_pools(
        blockchain = 'arbitrum'
        , project = 'fluid'
        , version = '1'
        , weth_address = '0x82aF49447D8a07e3bd95BD0d56f35241523fBab1'
    )
}}