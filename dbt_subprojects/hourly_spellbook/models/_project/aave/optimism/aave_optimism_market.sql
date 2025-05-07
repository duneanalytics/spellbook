{{
  config(
    schema = 'aave_optimism',
    alias = 'market',
    materialized = 'view'
  )
}}

{{
  lending_aave_compatible_market_view(
    blockchain = 'optimism'
  )
}}
