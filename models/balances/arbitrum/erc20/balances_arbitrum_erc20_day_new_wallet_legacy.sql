{{ config(
        tags = ['legacy'],
        alias = alias('day_new_wallet', legacy_model=True)
        )
}}

SELECT 
1 AS DUMMY 