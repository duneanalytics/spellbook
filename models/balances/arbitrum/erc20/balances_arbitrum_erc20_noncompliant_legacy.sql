{{ config(
        tags = ['erc20_legacy'],
        alias = alias('noncompliant', legacy_model=True)
        )
}}

SELECT 
1 AS DUMMY 