{{ config(
        schema = 'tokens_linea',
        alias = 'balances_daily',
        materialized = 'view',
        post_hook = '{{ expose_spells(
                        blockchains = \'["linea"]\',
                        spell_type = "sector",
                        spell_name = "balances",
                        contributors = \'["0xRob"]\') }}'
        )
}}

{{
balances_daily(
    balances_daily_agg = ref('tokens_linea_balances_daily_agg'),
    start_date = '2023-07-01',
    native_token = 'ETH'
)
}}
