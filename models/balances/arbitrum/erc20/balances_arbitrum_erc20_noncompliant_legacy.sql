{{ config(
        tags = ['legacy'],
        schema = 'balances_arbitrum_erc20',
        alias = alias('erc20_noncompliant', legacy_model=True)
        )
}}

SELECT 
1 AS DUMMY 