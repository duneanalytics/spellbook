{{
  config(
    schema = 'aave_scroll',
    alias = 'market',
    materialized = 'view'
  )
}}

{{
  lending_aave_compatible_market_view(
    blockchain = 'scroll'
  )
}}
