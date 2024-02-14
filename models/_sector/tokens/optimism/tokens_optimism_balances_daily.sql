{{ config(
        schema = 'tokens_optimism',
        alias = 'balances_daily',
        materialized = 'view'
        )
}}

{{
    balances_enrich(
        balances_base = ref('tokens_optimism_base_balances_daily'),
        blockchain = 'optimism',
        daily=true,
    )
}}
