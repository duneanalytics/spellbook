{{
  config(
    schema = 'aave_polygon',
    alias = 'market_hourly_agg',
    materialized = 'view'
  )
}}

{{
  lending_aave_compatible_market_hourly_agg_view(
    blockchain = 'polygon'
  )
}}
