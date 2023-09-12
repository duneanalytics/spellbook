{{ config(
        tags = ['legacy'],
        alias = alias('erc20_agg_day', legacy_model=True)
        )
}}

SELECT 
1 AS DUMMY 