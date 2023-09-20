{{ config(
        tags = ['legacy'],
        schema = 'balances_bnb_bep20',
        alias = alias('hour', legacy_model=True)
        )
}}

SELECT 
1 AS DUMMY 