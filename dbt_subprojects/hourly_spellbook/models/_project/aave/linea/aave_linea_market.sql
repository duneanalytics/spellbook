{{
  config(
    schema = 'aave_linea',
    alias = 'market',
    materialized = 'view'
  )
}}

{{
  lending_aave_compatible_market_view(
    blockchain = 'linea'
  )
}}
