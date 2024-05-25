{{ config(
        schema = 'tokens_zkevm',
        alias = 'balances_daily_agg',
        materialized = 'view'
        )
}}

{{
    balances_enrich(
        balances_raw = ref('tokens_zkevm_balances_daily_agg_base'),
        daily=true,
    )
}}
