{{ config(
        tags = ['legacy'],
        alias = alias('erc20_agg_hour', legacy_model=True)
        )
}}

SELECT 
1 AS DUMMY 