{{ config(
        schema = 'tokens_optimism',
        alias = 'balances_daily',
        materialized = 'view',
        post_hook = '{{ hide_spells() }}'
        )
}}

{{
balances_daily(
    balances_daily_agg = ref('tokens_optimism_balances_daily_agg'),
    start_date = '2021-01-14',
)
}}
