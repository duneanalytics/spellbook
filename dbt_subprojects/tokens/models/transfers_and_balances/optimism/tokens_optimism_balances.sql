{{ config(
        schema = 'tokens_optimism',
        alias = 'balances',
        materialized = 'view',
        post_hook = '{{ expose_spells(
                        blockchains = \'["optimism"]\',
                        spell_type = "sector",
                        spell_name = "balances",
                        contributors = \'["aalan3"]\') }}'
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
