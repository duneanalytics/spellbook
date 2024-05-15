{{ config(
        schema = 'balances_gnosis',
        
        alias = 'erc20_hour',
        post_hook='{{ expose_spells(\'["gnosis"]\',
                                    "sector",
                                    "balances",
                                    \'["hdser"]\') }}'
        )
}}

{{
    balances_fungible_hour(
        blockchain = 'gnosis',
        first_transaction_date = '2018-10-08',
        is_more_than_year_ago = true,
        transfers_rolling_hour = ref('transfers_gnosis_erc20_rolling_hour'),
        balances_noncompliant = ref('balances_gnosis_erc20_noncompliant')
    )
}}
