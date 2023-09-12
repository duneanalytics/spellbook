{{ config(
        tags = ['legacy'],
        alias = alias('matic_agg_day', legacy_model=True)
        )
}}

SELECT 
1 AS DUMMY 