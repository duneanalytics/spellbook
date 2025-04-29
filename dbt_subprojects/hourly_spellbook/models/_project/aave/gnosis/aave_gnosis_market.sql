{{
  config(
    schema = 'aave_gnosis',
    alias = 'market',
    materialized = 'view'
  )
}}

{{
  lending_aave_compatible_market_view(
    blockchain = 'gnosis'
  )
}}
