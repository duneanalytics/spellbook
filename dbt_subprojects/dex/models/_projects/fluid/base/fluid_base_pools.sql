{{
    config(
        schema = 'fluid_base',
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
        blockchain = 'base'
        , project = 'fluid'
        , version = '1'
        , weth_address = '0x4200000000000000000000000000000000000006'
    )
}}