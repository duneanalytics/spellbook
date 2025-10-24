{{ config(
    schema = 'tokens_base',
    alias = 'balances',
    materialized = 'view',
    post_hook = '{{ hide_spells() }}'
    )
}}

with balances_raw as (
{{balances_fix_schema(source('tokens_base', 'balances_base'),'base')}}
)

{{
    balances_enrich(
        balances_raw = 'balances_raw',
    )
}}
