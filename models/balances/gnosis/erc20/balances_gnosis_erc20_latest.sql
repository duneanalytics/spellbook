{{ config(
        schema = 'balances_gnosis',
        
        alias = 'erc20_latest',
        post_hook='{{ expose_spells(\'["gnosis"]\',
                                    "sector",
                                    "balances",
                                    \'["hdser"]\') }}'
        )
}}


{{
    balances_fungible_latest(
        blockchain = 'gnosis',
        transfers_rolling_hour = ref('transfers_gnosis_erc20_rolling_hour'),
        balances_noncompliant = ref('balances_gnosis_erc20_noncompliant')
    )
}}
