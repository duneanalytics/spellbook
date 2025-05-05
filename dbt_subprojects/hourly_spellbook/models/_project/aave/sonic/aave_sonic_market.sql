{{
  config(
    schema = 'aave_sonic',
    alias = 'market',
    materialized = 'view'
  )
}}

{{
  lending_aave_compatible_market_view(
    blockchain = 'sonic'
  )
}}
