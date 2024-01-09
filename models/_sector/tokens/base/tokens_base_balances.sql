{{ config(
        schema = 'tokens_base',
        alias = 'balances',
        materialized = 'view'
        )
}}

{{
    balances_enrich_raw(
        balances_base = source('tokens_base', 'balances_base_0001'),
        blockchain = 'base',
    )
}}
