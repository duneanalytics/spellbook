{{ config(
    schema = 'tokens_zksync',
    alias = 'balances_daily',
    materialized = 'view',
    post_hook = '{{ hide_spells() }}'
) }}

{{
    balances_daily(
        balances_daily_agg = ref('tokens_zksync_balances_daily_agg'),
        start_date = '2023-02-14',
    )
}}

