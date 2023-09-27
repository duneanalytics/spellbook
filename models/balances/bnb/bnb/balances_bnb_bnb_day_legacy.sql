{{ config(
        tags = ['legacy'],
        schema = 'balances_bnb',
        alias = alias('bnb_day', legacy_model=True)
        )
}}

SELECT 
1 AS DUMMY 