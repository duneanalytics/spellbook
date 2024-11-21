{{ config(
        schema = 'tokens_arbitrum',
        alias = 'balances_daily_agg',
        materialized = 'view'
        )
}}

{{
    balances_enrich(
        balances_raw = ref('tokens_arbitrum_balances_daily_agg_base'),
        daily=true,
    )
}}
