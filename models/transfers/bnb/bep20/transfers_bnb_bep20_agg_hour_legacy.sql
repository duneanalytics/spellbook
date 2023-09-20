{{ config(
        tags = ['legacy'],
        schema = 'transfers_bnb_bep20',
        alias = alias('bep20_agg_hour', legacy_model=True)
        )
}}

SELECT 
1 AS DUMMY 