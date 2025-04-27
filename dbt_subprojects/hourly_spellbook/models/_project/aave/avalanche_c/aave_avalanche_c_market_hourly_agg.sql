{{
  config(
    schema = 'aave_avalanche_c',
    alias = 'market_hourly_agg',
    materialized = 'view'
  )
}}

{{
  lending_aave_compatible_market_hourly_agg_view(
    blockchain = 'avalanche_c'
  )
}}
