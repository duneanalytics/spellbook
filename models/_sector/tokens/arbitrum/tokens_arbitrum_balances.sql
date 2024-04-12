{{ config(
        schema = 'tokens_arbitrum',
        alias = 'balances',
        materialized = 'view',
        post_hook = '{{ expose_spells(
                        blockchains = \'["arbitrum"]\',
                        spell_type = "sector",
                        spell_name = "balances",
                        contributors = \'["aalan3"]\') }}'
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
