{{ config(
    schema = 'dao',
    alias = alias('balances_steth', legacy_model=True),
    tags = ['legacy']
    )
}}

SELECT 

    1 as dummy 