{{ config(
    schema = 'tokens_kaia',
    alias = 'balances',
    materialized = 'view',
    post_hook = '{{ hide_spells() }}'
    )
}}

with balances_raw as (
{{balances_fix_schema(source('tokens_kaia', 'balances_kaia'),'kaia')}}
)

{{
    balances_enrich(
        balances_raw = 'balances_raw',
    )
}}
