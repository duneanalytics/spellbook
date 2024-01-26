{{ config(
        schema = 'tokens_arbitrum',
        alias = 'balances',
        materialized = 'view'
        )
}}

{{
    balances_enrich(
        balances_base = source('tokens_arbitrum', 'balances_arbitrum_0001'),
        blockchain = 'arbitrum',
    )
}}
