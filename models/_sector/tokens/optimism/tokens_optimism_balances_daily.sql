{{ config(
        schema = 'tokens_optimism',
        alias = 'balances_daily',
        materialized = 'view',
        post_hook = '{{ expose_spells(
                        blockchains = \'["optimism"]\',
                        spell_type = "sector",
                        spell_name = "balances",
                        contributors = \'["0xRob"]\') }}'
        )
}}

{{
balances_daily(
    balances_daily_agg = ref('tokens_optimism_balances_daily_agg'),
    start_date = '2021-01-14',
)
}}
