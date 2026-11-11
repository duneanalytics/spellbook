{{
  config(
    schema = 'aave_v3_fantom',
    alias = 'base_supply_scaled',
    materialized = 'table',
    file_format = 'delta',
    partition_by = ['block_month'],
    tags = ['static']
  )
}}

{{
  lending_aave_v3_compatible_supply_scaled(
    blockchain = 'fantom',
    project = 'aave',
    version = '3'
  )
}} 