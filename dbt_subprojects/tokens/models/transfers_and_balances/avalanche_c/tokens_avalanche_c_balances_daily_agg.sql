{{ config(
        schema = 'tokens_avalanche_c',
        alias = 'balances_daily_agg',
        materialized = 'view'
        )
}}

{{
    balances_enrich(
        balances_raw = ref('tokens_avalanche_c_balances_daily_agg_base'),
        daily=true,
    )
}}
