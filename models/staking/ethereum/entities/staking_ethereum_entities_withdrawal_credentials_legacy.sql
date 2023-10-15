{{
  config(
        schema = 'staking_ethereum',
        alias = alias('entities_withdrawal_credentials', legacy_model=True),
        tags=['legacy', 'static']
        )
}}

SELECT 1