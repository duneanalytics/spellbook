{{ config(
        tags = ['legacy'],
        alias = alias('rolling_day', legacy_model=True)
        )
}}

SELECT 
1 AS DUMMY 