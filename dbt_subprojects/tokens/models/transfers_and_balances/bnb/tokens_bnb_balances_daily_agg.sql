{{ config(
    schema = 'tokens_bnb',
    alias = 'balances_daily_agg',
    materialized = 'view',
    post_hook = '{{ deprecate_spells() }}'
) }}

{{
    balances_enrich(
        balances_raw = ref('tokens_bnb_balances_daily_agg_base'),
        daily = true,
    )
}}

