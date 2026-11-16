{{
  config(
    schema = 'aave_v3_fantom',
    alias = 'base_market_hourly_agg',
    materialized = 'table',
    tags = ['static'],
    file_format = 'delta',
  )
}}

{{
  lending_aave_v3_compatible_market_hourly_agg(
    blockchain = 'fantom'
  )
}}
