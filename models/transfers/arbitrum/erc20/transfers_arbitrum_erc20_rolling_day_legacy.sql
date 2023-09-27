{{ config(
        tags = ['legacy'],
        schema = 'transfers_arbitrum',
        alias = alias('rolling_day', legacy_model=True)
        )
}}

SELECT 
1 AS DUMMY 