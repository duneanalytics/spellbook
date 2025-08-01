{{ config(
        schema = 'tokens_scroll',
        alias = 'balances_daily',
        materialized = 'view',
        post_hook = '{{ hide_spells() }}'
        )
}}

{{
balances_daily(
    balances_daily_agg = ref('tokens_scroll_balances_daily_agg'),
    start_date = '2023-10-10',
)
}}
