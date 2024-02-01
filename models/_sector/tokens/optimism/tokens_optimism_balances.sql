{{ config(
        schema = 'tokens_optimism',
        alias = 'balances',
        materialized = 'view'
        )
}}

{{
    balances_enrich(
        balances_base = source('tokens_optimism', 'balances_optimism_0001'),
        blockchain = 'optimism',
    )
}}
