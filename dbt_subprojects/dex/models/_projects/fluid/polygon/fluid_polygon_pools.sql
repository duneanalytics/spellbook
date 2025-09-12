{{
    config(
        schema = 'fluid_polygon',
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
        blockchain = 'polygon'
        , project = 'fluid'
        , version = '1'
        , weth_address = '0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270'
    )
}}