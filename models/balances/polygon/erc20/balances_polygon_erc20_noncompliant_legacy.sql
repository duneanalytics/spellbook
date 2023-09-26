{{ config(
        schema = 'balances_polygon',
        tags = ['legacy'],
        alias = alias('erc20_noncompliant', legacy_model=True)
        )
}}

SELECT 
1 AS DUMMY 