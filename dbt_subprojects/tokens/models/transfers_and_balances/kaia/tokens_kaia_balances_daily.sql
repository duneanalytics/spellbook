{{ config(
        schema = 'tokens_kaia',
        alias = 'balances_daily',
        materialized = 'view',
        post_hook = '{{ hide_spells() }}'
        )
}}

{{
balances_daily(
    balances_daily_agg = ref('tokens_kaia_balances_daily_agg'),
    start_date = '2019-06-24',
)
}}
