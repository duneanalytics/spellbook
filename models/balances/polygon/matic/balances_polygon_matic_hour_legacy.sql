{{ config(
        tags = ['legacy'],
        alias = alias('matic_hour', legacy_model=True)
        )
}}

SELECT 
1 AS DUMMY 