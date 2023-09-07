{{ config(
        tags = ['legacy'],
        alias = alias('ftm_latest', legacy_model=True)
        )
}}

SELECT 
1 AS DUMMY 