{{
  config(
        schema = 'staking_ethereum',
        alias = alias('entities_tx_from_addresses', legacy_model=True),
        tags=['legacy', 'static']
        )
}}

SELECT 1