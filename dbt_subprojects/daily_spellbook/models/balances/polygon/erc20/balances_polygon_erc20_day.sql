{{ config(
        schema = 'balances_polygon',
        
        alias = 'erc20_day',
        post_hook='{{ hide_spells() }}'
        )
}}


{{
    balances_fungible_day(
        blockchain = 'polygon',
        first_transaction_date = '2020-05-30',
        transfers_rolling_day = ref('transfers_polygon_erc20_rolling_day'),
        balances_noncompliant = ref('balances_polygon_erc20_noncompliant'),
        filter_mainnet_token = '0x0000000000000000000000000000000000001010'
    )
}}
