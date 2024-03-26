{{ config(
    schema = 'tokens_base',
    alias = 'balances',
    materialized = 'view',
    post_hook = '{{ expose_spells(
                        blockchains = \'["base"]\',
                        spell_type = "sector",
                        spell_name = "balances",
                        contributors = \'["aalan3"]\') }}'
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
