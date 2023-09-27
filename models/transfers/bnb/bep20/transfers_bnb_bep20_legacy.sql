{{ config(
        tags = ['legacy'],
        schema = 'transfers_bnb',
        alias = alias('bep20', legacy_model=True)
        )
}}

SELECT 
1 AS DUMMY 
