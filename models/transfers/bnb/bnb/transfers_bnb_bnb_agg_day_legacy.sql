{{ config(
        tags = ['legacy'],
        schema = 'transfers_bnb_bnb',
        alias = alias('bnb_agg_day', legacy_model=True)
        )
}}

SELECT 
1 AS DUMMY 