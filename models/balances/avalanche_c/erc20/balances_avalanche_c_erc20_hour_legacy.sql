{{ config(
        tags = ['legacy'],
        alias = alias('hour', legacy_model=True)
        )
}}

SELECT 
1 AS DUMMY 