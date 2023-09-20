{{ config(
        tags = ['dunesql'],
        schema = 'balances_arbitrum_erc20',
        alias = alias('erc20_latest'),
        post_hook='{{ expose_spells(\'["arbitrum"]\',
                                    "sector",
                                    "balances",
                                    \'["Henrystats"]\') }}'
        )
}}

{{
    balances_fungible_latest(
        blockchain = 'arbitrum',
        transfers_rolling_hour = ref('transfers_arbitrum_erc20_rolling_hour'),
        balances_noncompliant = ref('balances_arbitrum_erc20_noncompliant')
    )
}}
