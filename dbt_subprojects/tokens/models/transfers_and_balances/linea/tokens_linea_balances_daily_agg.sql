{{ config(
        schema = 'tokens_linea',
        alias = 'balances_daily_agg',
        materialized = 'view',
        post_hook = '{{ deprecate_spells() }}'
        )
}}

{{
    balances_enrich(
        balances_raw = ref('tokens_linea_balances_daily_agg_base'),
        daily=true,
    )
}}
