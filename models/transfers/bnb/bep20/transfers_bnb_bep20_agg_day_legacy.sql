{{ config(
        tags = ['legacy'],
        schema = 'transfers_bnb',
        alias = alias('bep20_agg_day', legacy_model=True)
        )
}}

SELECT 
1 AS DUMMY 