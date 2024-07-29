{{ config(
        schema = 'tokens_zksync',
        alias = 'balances_daily_agg',
        materialized = 'view'
        )
}}

{{
    balances_enrich(
        balances_raw = ref('tokens_zksync_balances_daily_agg_base'),
        daily=true,
    )
}}
