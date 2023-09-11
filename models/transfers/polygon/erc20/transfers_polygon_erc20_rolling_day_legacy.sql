{{ config(
        tags = ['legacy'],
        alias = alias('erc20_rolling_day', legacy_model=True)
        )
}}

SELECT 
1 AS DUMMY 