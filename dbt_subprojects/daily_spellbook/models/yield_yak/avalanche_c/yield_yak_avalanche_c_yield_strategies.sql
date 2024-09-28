{{
    config(
        schema = 'yield_yak_avalanche_c',
        alias = 'yield_strategies',
        materialized = 'view'
    )
}}

{{
    yield_yak_yield_strategies(
        blockchain = 'avalanche_c'
    )
}}
