{{ config(
        tags = ['legacy'],
        schema = 'transfers_bnb_bnb',
        alias = alias('bnb', legacy_model=True)
        )
}}

SELECT 
1 AS DUMMY 
