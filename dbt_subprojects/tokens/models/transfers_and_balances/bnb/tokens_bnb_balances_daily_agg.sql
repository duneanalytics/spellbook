{{ config(
        schema = 'tokens_bnb',
        alias = 'balances_daily_agg',
        materialized = 'view'
        )
}}

{{
    balances_enrich(
        balances_raw = ref('tokens_bnb_balances_daily_agg_base'),
        daily=true,
    )
}}
