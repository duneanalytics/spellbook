{{ config(
        schema = 'tokens_scroll',
        alias = 'balances_daily',
        materialized = 'view',
        post_hook = '{{ expose_spells(\'["scroll"]\',
                                    "sector",
                                    "tokens",
                                    \'["0xRob"]\') }}'
        )
}}

{{
balances_daily(
    balances_daily_agg = ref('tokens_scroll_balances_daily_agg'),
    start_date = '2023-10-10',
)
}}
