{{ config(
        schema = 'tokens_kaia',
        alias = 'balances_daily_agg',
        materialized = 'view'
        )
}}

{{
    balances_enrich(
        balances_raw = ref('tokens_kaia_balances_daily_agg_base'),
        daily=true,
    )
}}
