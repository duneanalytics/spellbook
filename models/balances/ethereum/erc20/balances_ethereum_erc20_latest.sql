{{ config(
        schema = 'balances_ethereum',
        
        alias = 'erc20_latest',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "sector",
                                    "balances",
                                    \'["Henrystats"]\') }}'
        )
}}


{{
    balances_fungible_latest(
        blockchain = 'ethereum',
        transfers_rolling_hour = ref('transfers_ethereum_erc20_rolling_hour'),
        balances_noncompliant = ref('balances_ethereum_erc20_noncompliant')
    )
}}

