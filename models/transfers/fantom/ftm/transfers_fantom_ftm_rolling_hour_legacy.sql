{{ config(
        tags = ['legacy'],
        alias = alias('ftm_rolling_hour', legacy_model=True)
        )
}}

SELECT 
1 AS DUMMY 