{{ config(
        tags = ['legacy'],
        schema = 'balances_arbitrum',
        alias = alias('erc20_noncompliant', legacy_model=True)
        )
}}

SELECT 
1 AS DUMMY 