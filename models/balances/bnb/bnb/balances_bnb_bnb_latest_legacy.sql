{{ config(
        tags = ['legacy'],
        alias = alias('bnb_latest', legacy_model=True)
        )
}}

SELECT 
1 AS DUMMY 