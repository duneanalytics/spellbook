{{ config(
        tags = ['legacy'],
        alias = alias('agg_day', legacy_model=True)
        )
}}

SELECT 
1 AS DUMMY 