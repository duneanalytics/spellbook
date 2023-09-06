{{ config(
        tags = ['legacy'],
        alias = alias('avax_day', legacy_model=True)
        )
}}

SELECT 
1 AS DUMMY 