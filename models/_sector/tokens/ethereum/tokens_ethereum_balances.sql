{{ config(
        schema = 'tokens_ethereum',
        alias = 'balances',
        materialized = 'view'
        )
}}

{{
    balances_enrich_raw(
        balances_base = source('tokens_ethereum', 'balances_ethereum_0002'),
        blockchain = 'ethereum',
    )
}}