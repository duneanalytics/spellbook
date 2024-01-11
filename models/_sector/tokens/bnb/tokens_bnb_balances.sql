{{ config(
        schema = 'tokens_bnb',
        alias = 'balances',
        materialized = 'view'
        )
}}

{{
    balances_enrich(
        balances_base = source('tokens_bnb', 'balances_bnb_0001'),
        blockchain = 'bnb',
    )
}}
