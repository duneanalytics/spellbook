{{ config(
        schema = 'tokens_kaia',
        alias = 'balances_daily',
        materialized = 'view',
        post_hook = '{{ expose_spells(
                        blockchains = \'["kaia"]\',
                        spell_type = "sector",
                        spell_name = "balances",
                        contributors = \'["0xRob"]\') }}'
        )
}}

{{
balances_daily(
    balances_daily_agg = ref('tokens_kaia_balances_daily_agg'),
    start_date = '2019-06-24',
)
}}
