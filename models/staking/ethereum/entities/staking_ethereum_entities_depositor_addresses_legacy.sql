{{
  config(
        schema = 'staking_ethereum',
        alias = alias('entities_depositor_addresses', legacy_model=True),
        tags=['legacy', 'static']
        )
}}

SELECT 1