{{ config(
        tags = ['legacy'],
        alias = alias('eth', legacy_model=True)
        )
}}

SELECT 
1 AS DUMMY 
