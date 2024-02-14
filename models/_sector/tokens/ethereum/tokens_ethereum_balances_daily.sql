{{ config(
        schema = 'tokens_ethereum',
        alias = 'balances_daily',
        materialized = 'view'
        )
}}

{{
    balances_enrich(
        balances_base = ref('tokens_ethereum_base_balances_daily'),
        blockchain = 'ethereum',
        daily=true,
    )
}}
