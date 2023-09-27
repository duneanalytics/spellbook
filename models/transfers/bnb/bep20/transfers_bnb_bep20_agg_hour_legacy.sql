{{ config(
        tags = ['legacy'],
        schema = 'transfers_bnb',
        alias = alias('bep20_agg_hour', legacy_model=True)
        )
}}

SELECT 
1 AS DUMMY 