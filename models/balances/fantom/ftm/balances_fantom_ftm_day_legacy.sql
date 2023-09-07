{{ config(
        tags = ['legacy'],
        alias = alias('ftm_day', legacy_model=True)
        )
}}

SELECT 
1 AS DUMMY 