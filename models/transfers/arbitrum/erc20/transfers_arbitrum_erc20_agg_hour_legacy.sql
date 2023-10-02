{{ config(
        tags = ['legacy'],
        alias = alias('agg_hour', legacy_model=True)
        )
}}

SELECT 
1 AS DUMMY 