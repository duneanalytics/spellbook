{{ config(
        tags = ['legacy'],
        alias = alias('erc20_supply', legacy_model=True)
        )
}}

SELECT 
1 AS DUMMY 