{{ config(
        schema = 'tokens_optimism',
        alias = 'balances',
        materialized = 'view',
        post_hook = '{{ hide_spells() }}'
        )
}}

with balances_raw as (
{{balances_fix_schema(source('tokens_optimism', 'balances_optimism'),'optimism')}}
)

{{
    balances_enrich(
        balances_raw = 'balances_raw',
    )
}}
