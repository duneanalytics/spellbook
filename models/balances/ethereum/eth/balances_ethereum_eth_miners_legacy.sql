{{ config(
        schema = 'balances_ethereum',
        tags = ['legacy'],
        alias = alias('eth_miners', legacy_model=True)
        )
}}

SELECT 
1 AS DUMMY 