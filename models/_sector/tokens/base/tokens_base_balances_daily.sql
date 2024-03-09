{{ config(
        schema = 'tokens_base',
        alias = 'balances_daily',
        materialized = 'view',
        post_hook = '{{ expose_spells(\'["base"]\',
                                    "sector",
                                    "tokens",
                                    \'["0xRob"]\') }}'
        )
}}

{{
balances_daily(
    balances_daily_agg = ref('tokens_base_balances_daily_agg'),
    start_date = '2023-07-15',
)
}}
