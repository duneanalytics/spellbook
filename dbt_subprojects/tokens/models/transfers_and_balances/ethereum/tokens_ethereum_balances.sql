{{ config(
        schema = 'tokens_ethereum',
        alias = 'balances',
        materialized = 'view',
        post_hook = '{{ hide_spells() }}'
        )
}}

with balances_raw as (
{{balances_fix_schema(source('tokens_ethereum', 'balances_ethereum'), 'ethereum')}}
)

{{
    balances_enrich(
        balances_raw = 'balances_raw',
    )
}}
