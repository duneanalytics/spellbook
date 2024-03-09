{{ config(
        schema = 'tokens_scroll',
        alias = 'balances_daily_agg',
        materialized = 'view'
        )
}}

{{
    balances_enrich(
        balances_raw = ref('tokens_scroll_balances_daily_agg_base'),
        daily=true,
    )
}}
