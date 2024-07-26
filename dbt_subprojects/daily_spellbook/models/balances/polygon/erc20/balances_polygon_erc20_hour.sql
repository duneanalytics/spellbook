{{ config(
        schema = 'balances_polygon',
        
        alias = 'erc20_hour',
        post_hook='{{ expose_spells(\'["polygon"]\',
                                    "sector",
                                    "balances",
                                    \'["Henrystats"]\') }}'
        )
}}

{{
    balances_fungible_hour(
        blockchain = 'polygon',
        first_transaction_date = '2020-05-30',
        is_more_than_year_ago = true,
        transfers_rolling_hour = ref('transfers_polygon_erc20_rolling_hour'),
        balances_noncompliant = ref('balances_polygon_erc20_noncompliant'),
        filter_mainnet_token = '0x0000000000000000000000000000000000001010'
    )
}}
