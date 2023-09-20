{{ config(
        schema = 'balances_ethereum_eth',
        tags = ['legacy'],
        alias = alias('eth_latest', legacy_model=True)
        )
}}

SELECT 
1 AS DUMMY 