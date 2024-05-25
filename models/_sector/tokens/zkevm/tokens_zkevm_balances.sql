{{ config(
        schema = 'tokens_zkevm',
        alias = 'balances',
        materialized = 'view',
        post_hook = '{{ expose_spells(
                        blockchains = \'["zkevm"]\',
                        spell_type = "sector",
                        spell_name = "balances",
                        contributors = \'["aalan3", "hildobby"]\') }}'
        )
}}

with balances_raw as (
{{balances_fix_schema(source('tokens_zkevm', 'balances_zkevm'), 'zkevm')}}
)

{{
    balances_enrich(
        balances_raw = 'balances_raw',
    )
}}
