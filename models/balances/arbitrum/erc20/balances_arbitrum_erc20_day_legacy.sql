{{ config(
        tags = ['legacy'],
        alias = alias('erc20_day', legacy_model=True)
        )
}}

SELECT 
1 AS DUMMY 