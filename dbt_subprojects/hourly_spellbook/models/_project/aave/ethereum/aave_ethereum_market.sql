{{
  config(
    schema = 'aave_ethereum',
    alias = 'market',
    materialized = 'view'
  )
}}

{{
  lending_aave_compatible_market_view(
    blockchain = 'ethereum'
  )
}}
