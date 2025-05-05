{{
  config(
    schema = 'aave_bnb',
    alias = 'market',
    materialized = 'view'
  )
}}

{{
  lending_aave_compatible_market_view(
    blockchain = 'bnb'
  )
}}
