{{ config(
        schema = 'balances_gnosis',
        
        alias = 'erc20_day',
        post_hook='{{ expose_spells(\'["gnosis"]\',
                                    "sector",
                                    "balances",
                                    \'["hdser"]\') }}'
        )
}}


{{
    balances_fungible_day(
        blockchain = 'gnosis',
        first_transaction_date = '2018-10-08',
        transfers_rolling_day = ref('transfers_gnosis_erc20_rolling_day'),
        balances_noncompliant = ref('balances_gnosis_erc20_noncompliant')
    )
}}
