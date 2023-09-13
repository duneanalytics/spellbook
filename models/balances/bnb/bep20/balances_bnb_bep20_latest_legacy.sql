{{ config(
        tags = ['legacy'],
        alias = alias('latest', legacy_model=True)
        )
}}

SELECT 
1 AS DUMMY 