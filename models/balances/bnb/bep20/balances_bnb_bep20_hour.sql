{{ config(
        tags = ['dunesql'],
        schema = 'balances_bnb_bep20',
        alias = alias('bep20_hour'),
        post_hook='{{ expose_spells(\'["bnb"]\',
                                    "sector",
                                    "balances",
                                    \'["Henrystats"]\') }}'
        )
}}


{{
    balances_fungible_hour(
        blockchain = 'bnb',
        first_transaction_date = '2020-08-29',
        is_more_than_year_ago = true,
        transfers_rolling_hour = ref('transfers_bnb_bep20_rolling_hour'),
        balances_noncompliant = ref('balances_bnb_bep20_noncompliant')
    )
}}
