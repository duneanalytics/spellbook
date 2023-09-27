{{ config(
        tags = ['legacy'],
        schema = 'balances_bnb',
        alias = alias('bep20_latest', legacy_model=True)
        )
}}

SELECT 
1 AS DUMMY 