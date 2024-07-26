{{ config(
        schema = 'tokens_optimism',
        alias = 'balances_daily_agg',
        materialized = 'view'
        )
}}

{{
    balances_enrich(
        balances_raw = ref('tokens_optimism_balances_daily_agg_base'),
        daily=true,
    )
}}
