{{ config(
        schema = 'tokens_zkevm',
        alias = 'balances_daily',
        materialized = 'view',
        post_hook = '{{ expose_spells(
                        blockchains = \'["zkevm"]\',
                        spell_type = "sector",
                        spell_name = "balances",
                        contributors = \'["0xRob", "hildobby"]\') }}'
        )
}}

{{
balances_daily(
    balances_daily_agg = ref('tokens_zkevm_balances_daily_agg'),
    start_date = '2023-10-10',
)
}}
