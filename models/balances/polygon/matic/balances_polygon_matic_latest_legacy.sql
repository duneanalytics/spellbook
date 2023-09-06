{{ config(
        tags = ['legacy'],
        alias = alias('matic_latest', legacy_model=True)
        )
}}

SELECT 
1 AS DUMMY 