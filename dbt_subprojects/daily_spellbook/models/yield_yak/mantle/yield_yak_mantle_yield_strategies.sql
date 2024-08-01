{{
    config(
        schema = 'yield_yak_mantle',
        alias = 'yield_strategies',
        materialized = 'view'
    )
}}

{{
    yield_yak_yield_strategies(
        blockchain = 'mantle'
    )
}}
