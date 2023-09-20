{{ config(
        tags = ['dunesql'],
        schema = 'balances_bnb_bep20',
        alias = alias('bep20_day'),
        post_hook='{{ expose_spells(\'["bnb"]\',
                                    "sector",
                                    "balances",
                                    \'["Henrystats"]\') }}'
        )
}}

{{
    balances_fungible_day(
        blockchain = 'bnb',
        first_transaction_date = '2020-08-29',
        transfers_rolling_day = ref('transfers_bnb_bep20_rolling_day'),
        balances_noncompliant = ref('balances_bnb_bep20_noncompliant')
    )
}}
