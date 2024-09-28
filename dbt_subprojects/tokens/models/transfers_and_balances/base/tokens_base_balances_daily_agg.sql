{{ config(
        schema = 'tokens_base',
        alias = 'balances_daily_agg',
        materialized = 'view'
        )
}}

{{
    balances_enrich(
        balances_raw = ref('tokens_base_balances_daily_agg_base'),
        daily=true,
    )
}}
