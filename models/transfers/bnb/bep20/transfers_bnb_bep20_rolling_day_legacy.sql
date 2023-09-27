{{ config(
        tags = ['legacy'],
        schema = 'transfers_bnb',
        alias = alias('bep20_rolling_day', legacy_model=True)
        )
}}

SELECT 
1 AS DUMMY 