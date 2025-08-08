{{ config(
        schema = 'tokens_arbitrum',
        alias = 'balances',
        materialized = 'view',
        post_hook = '{{ hide_spells() }}'
        )
}}

with balances_raw as (
{{balances_fix_schema(source('tokens_arbitrum', 'balances_arbitrum'),'arbitrum')}}
)

{{
    balances_enrich(
        balances_raw = 'balances_raw',
    )
}}
