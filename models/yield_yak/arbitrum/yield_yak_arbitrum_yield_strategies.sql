{{
    config(
        schema = 'yield_yak_arbitrum',
        alias = 'yield_strategies',
        materialized = 'view'
    )
}}

{{
    yield_yak_yield_strategies(
        blockchain = 'arbitrum'
    )
}}
