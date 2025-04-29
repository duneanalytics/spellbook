{{
  config(
    schema = 'aave_celo',
    alias = 'market',
    materialized = 'view'
  )
}}

{{
  lending_aave_compatible_market_view(
    blockchain = 'celo'
  )
}}
