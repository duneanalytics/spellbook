{{ config(
        schema = 'tokens_linea',
        alias = 'balances_daily',
        materialized = 'view',
        post_hook = '{{ hide_spells() }}'
        )
}}

{{
balances_daily(
    balances_daily_agg = ref('tokens_linea_balances_daily_agg'),
    start_date = '2023-07-01',
    native_token = 'ETH'
)
}}
