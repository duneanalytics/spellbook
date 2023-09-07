{{ config(
        tags = ['legacy'],
        alias = alias('ftm_agg_hour', legacy_model=True)
        )
}}

SELECT 
1 AS DUMMY 