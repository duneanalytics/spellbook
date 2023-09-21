{{ config(
        schema = 'balances_ethereum_eth',
        tags = ['legacy'],
        alias = alias('eth_suicide', legacy_model=True)
        )
}}

SELECT 
1 AS DUMMY 