{{
  config(
    schema = 'aave_gnosis',
    alias = 'market_hourly_agg',
    materialized = 'view'
  )
}}

{{
  lending_aave_compatible_market_hourly_agg_view(
    blockchain = 'gnosis'
  )
}}
