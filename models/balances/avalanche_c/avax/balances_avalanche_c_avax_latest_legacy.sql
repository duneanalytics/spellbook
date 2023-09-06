{{ config(
        tags = ['legacy'],
        alias = alias('avax_latest', legacy_model=True)
        )
}}

SELECT 
1 AS DUMMY 