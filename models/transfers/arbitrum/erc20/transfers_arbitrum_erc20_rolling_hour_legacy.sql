{{ config(
        tags = ['legacy'],
        schema = 'transfers_arbitrum',
        alias = alias('rolling_hour', legacy_model=True)
        )
}}

SELECT 
1 AS DUMMY 