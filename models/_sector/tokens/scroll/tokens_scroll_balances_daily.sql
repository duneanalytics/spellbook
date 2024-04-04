{{ config(
        schema = 'tokens_scroll',
        alias = 'balances_daily',
        materialized = 'view',
        post_hook = '{{ expose_spells(
                        blockchains = \'["scroll"]\',
                        spell_type = "sector",
                        spell_name = "balances",
                        contributors = \'["0xRob"]\') }}'
        )
}}

{{
balances_daily(
    balances_daily_agg = ref('tokens_scroll_balances_daily_agg'),
    start_date = '2023-10-10',
)
}}
