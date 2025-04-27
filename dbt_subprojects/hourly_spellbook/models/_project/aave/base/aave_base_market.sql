{{
  config(
    schema = 'aave_base',
    alias = 'market',
    materialized = 'view'
  )
}}

{{
  lending_aave_compatible_market_view(
    blockchain = 'base'
  )
}}
