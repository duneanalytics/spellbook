{{ config(
        tags = ['legacy'],
        alias = alias('eth_hour', legacy_model=True)
        )
}}

SELECT 
1 AS DUMMY 