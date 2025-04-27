{{
  config(
    schema = 'aave_sonic',
    alias = 'market_hourly_agg',
    materialized = 'view'
  )
}}

{{
  lending_aave_compatible_market_hourly_agg_view(
    blockchain = 'sonic'
  )
}}
