{{ config(
        schema = 'tokens_avalanche_c',
        alias = 'balances_daily',
        materialized = 'view',
        post_hook = '{{ hide_spells() }}'
        )
}}

{{
balances_daily(
    balances_daily_agg = ref('tokens_avalanche_c_balances_daily_agg'),
    start_date = '2020-09-25',
    native_token = 'AVAX'
)
}}
