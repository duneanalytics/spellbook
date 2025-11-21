{{ config(
        schema = 'tokens_base',
        alias = 'balances_daily',
        materialized = 'view',
        post_hook = '{{ hide_spells() }}'
        )
}}

{{
balances_daily(
    balances_daily_agg = ref('tokens_base_balances_daily_agg'),
    start_date = '2023-07-15',
)
}}
