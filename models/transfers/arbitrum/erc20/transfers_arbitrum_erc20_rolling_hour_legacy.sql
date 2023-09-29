{{ config(
        tags = ['legacy'],
        alias = alias('rolling_hour', legacy_model=True)
        )
}}

SELECT 
1 AS DUMMY 