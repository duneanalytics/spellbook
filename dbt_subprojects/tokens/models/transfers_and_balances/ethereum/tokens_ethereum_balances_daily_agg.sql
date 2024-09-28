{{ config(
        schema = 'tokens_ethereum',
        alias = 'balances_daily_agg',
        materialized = 'view'
        )
}}

{{
    balances_enrich(
        balances_raw = ref('tokens_ethereum_balances_daily_agg_base'),
        daily=true,
    )
}}
