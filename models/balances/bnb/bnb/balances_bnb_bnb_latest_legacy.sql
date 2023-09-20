{{ config(
        tags = ['legacy'],
        schema = 'balances_bnb_bnb',
        alias = alias('bnb_latest', legacy_model=True)
        )
}}

SELECT 
1 AS DUMMY 