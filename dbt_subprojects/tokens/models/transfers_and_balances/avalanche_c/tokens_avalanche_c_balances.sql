{{ config(
        schema = 'tokens_avalanche_c',
        alias = 'balances',
        materialized = 'view',
        post_hook = '{{ hide_spells() }}'
        )
}}

with balances_raw as (
{{balances_fix_schema(source('tokens_avalanche_c', 'balances_avalanche_c'),'avalanche_c', '0x0000000000000000000000000000000000000000')}}
)

{{
    balances_enrich(
        balances_raw = 'balances_raw',
    )
}}
