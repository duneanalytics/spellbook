{{ config(
        tags = ['legacy'],
        alias = alias('ftm', legacy_model=True)
        )
}}

SELECT 
1 AS DUMMY 
