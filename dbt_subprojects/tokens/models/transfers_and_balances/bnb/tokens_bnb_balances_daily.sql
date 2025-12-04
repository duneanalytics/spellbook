{{ config(
    tags = ['prod_exclude'],
    schema = 'tokens_bnb',
    alias = 'balances_daily',
    materialized = 'view',
    post_hook = '{{ hide_spells() }}'
) }}

{{
    balances_daily(
        balances_daily_agg = ref('tokens_bnb_balances_daily_agg'),
        start_date = '2020-08-29',
        native_token = 'BNB'
    )
}}

