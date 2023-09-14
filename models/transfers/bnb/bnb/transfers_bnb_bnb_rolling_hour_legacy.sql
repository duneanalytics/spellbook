{{ config(
        tags = ['legacy'],
        alias = alias('bnb_rolling_hour', legacy_model=True)
        )
}}

SELECT 
1 AS DUMMY 