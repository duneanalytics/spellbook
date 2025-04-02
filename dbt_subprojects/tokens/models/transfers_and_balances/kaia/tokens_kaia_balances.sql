{{ config(
    schema = 'tokens_kaia',
    alias = 'balances',
    materialized = 'view',
    post_hook = '{{ expose_spells(
                        blockchains = \'["kaia"]\',
                        spell_type = "sector",
                        spell_name = "balances",
                        contributors = \'["aalan3"]\') }}'
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
