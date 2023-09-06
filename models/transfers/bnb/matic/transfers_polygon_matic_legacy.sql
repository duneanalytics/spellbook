{{ config(
        tags = ['legacy'],
        alias = alias('matic', legacy_model=True)
        )
}}

SELECT 
1 AS DUMMY 
