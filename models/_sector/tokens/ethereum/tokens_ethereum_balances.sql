{{ config(
        schema = 'tokens_ethereum',
        alias = 'balances',
        materialized = 'view'
        )
}}

{{
    balances_enrich(
        balances_base = source('tokens_ethereum', 'balances_ethereum_0004'),
        blockchain = 'ethereum',
    )
}}
