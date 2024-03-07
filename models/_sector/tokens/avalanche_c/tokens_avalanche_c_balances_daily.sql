{{ config(
        schema = 'tokens_avalanche_c',
        alias = 'balances_daily',
        materialized = 'view',
        post_hook = '{{ expose_spells(\'["avalanche_c"]\',
                                    "sector",
                                    "tokens",
                                    \'["0xRob"]\') }}'
        )
}}

{{
balances_daily(
    balances_daily_agg = ref('tokens_avalanche_c_balances_daily_agg'),
    start_date = '2020-09-25',
    native_token = 'AVAX'
)
}}
