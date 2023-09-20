{{ config(
        tags = ['legacy'],
        schema = 'balances_bnb_bnb',
        alias = alias('bnb_hour', legacy_model=True)
        )
}}

SELECT 
1 AS DUMMY 