{{ config(
        tags = ['dunesql'],
        schema = 'balances_bnb',
        alias = alias('bep20_latest'),
        post_hook='{{ expose_spells(\'["bnb"]\',
                                    "sector",
                                    "balances",
                                    \'["Henrystats"]\') }}'
        )
}}

{{
    balances_fungible_latest(
        blockchain = 'bnb',
        transfers_rolling_hour = ref('transfers_bnb_bep20_rolling_hour'),
        balances_noncompliant = ref('balances_bnb_bep20_noncompliant')
    )
}}