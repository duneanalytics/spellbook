{{ config(
        schema = 'tokens_base',
        alias = 'balances_daily',
        materialized = 'view'
        )
}}

{{
    balances_enrich(
        balances_base = ref('tokens_base_base_balances_daily'),
        blockchain = 'base',
        daily=true,
    )
}}
