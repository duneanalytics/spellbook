{{
  config(
    schema = 'aave_polygon',
    alias = 'market',
    materialized = 'view'
  )
}}

{{
  lending_aave_compatible_market_view(
    blockchain = 'polygon'
  )
}}
