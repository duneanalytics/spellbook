{{ config(
        schema = 'tokens_worldchain',
        alias = 'balances_daily',
        materialized = 'view',
        post_hook = '{{ deprecate_spells() }}'
        )
}}

{{
balances_daily(
    balances_daily_agg = ref('tokens_worldchain_balances_daily_agg'),
    start_date = '2024-06-25',
)
}}
