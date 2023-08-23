{{ config(
    tags=['legacy'],
    alias = alias('accounts',legacy_model=True)
    )
}}

SELECT 1 as account_address