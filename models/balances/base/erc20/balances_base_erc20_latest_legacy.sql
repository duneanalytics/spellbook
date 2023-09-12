{{ config(
        tags = ['legacy'],
        alias = alias('erc20_latest', legacy_model=True)
        )
}}

SELECT 
1 AS DUMMY 