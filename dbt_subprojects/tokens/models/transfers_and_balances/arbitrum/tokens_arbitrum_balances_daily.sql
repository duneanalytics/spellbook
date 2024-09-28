{{ config(
        schema = 'tokens_arbitrum',
        alias = 'balances_daily',
        materialized = 'view',
        post_hook = '{{ expose_spells(
                        blockchains = \'["arbitrum"]\',
                        spell_type = "sector",
                        spell_name = "balances",
                        contributors = \'["0xRob"]\') }}'
        )
}}

{{
balances_daily(
    balances_daily_agg = ref('tokens_arbitrum_balances_daily_agg'),
    start_date = '2021-05-28',
)
}}
