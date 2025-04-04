{{ config(
        schema = 'tokens_worldchain',
        alias = 'balances_daily',
        materialized = 'view',
        post_hook = '{{ expose_spells(
                        blockchains = \'["worldchain"]\',
                        spell_type = "sector",
                        spell_name = "balances",
                        contributors = \'["0xRob"]\') }}'
        )
}}

{{
balances_daily(
    balances_daily_agg = ref('tokens_worldchain_balances_daily_agg'),
    start_date = '2024-06-25',
)
}}
