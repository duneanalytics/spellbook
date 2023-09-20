{{ config(
        schema = 'balances_polygon_erc20',
        tags = ['dunesql'],
        alias = alias('erc20_latest'),
        post_hook='{{ expose_spells(\'["polygon"]\',
                                    "sector",
                                    "balances",
                                    \'["Henrystats"]\') }}'
        )
}}


{{
    balances_fungible_latest(
        blockchain = 'polygon',
        transfers_rolling_hour = ref('transfers_polygon_erc20_rolling_hour'),
        balances_noncompliant = ref('balances_polygon_erc20_noncompliant'),
        filter_mainnet_token = '0x0000000000000000000000000000000000001010'
    )
}}
