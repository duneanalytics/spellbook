{{
  config(
        schema = 'staking_ethereum',
        alias = alias('entities_batch_contracts_tx_from', legacy_model=True),
        tags=['legacy', 'static']
        )
}}

SELECT 1