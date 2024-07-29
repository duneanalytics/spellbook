{{ config(
        schema = 'tokens_zksync',
        alias = 'balances',
        materialized = 'view',
        post_hook = '{{ expose_spells(
                        blockchains = \'["zksync"]\',
                        spell_type = "sector",
                        spell_name = "balances",
                        contributors = \'["aalan3"]\') }}'
        )
}}

with balances_raw as (
{{balances_fix_schema(source('tokens_zksync', 'balances_zksync'),'zksync')}}
)

{{
    balances_enrich(
        balances_raw = 'balances_raw',
    )
}}
