{{
  config(
        schema = 'staking_ethereum',
        alias = alias('entities_coinbase', legacy_model=True),
        tags=['legacy', 'static']
        )
}}

SELECT 1