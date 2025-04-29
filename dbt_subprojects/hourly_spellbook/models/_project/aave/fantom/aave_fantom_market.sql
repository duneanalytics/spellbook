{{
  config(
    schema = 'aave_fantom',
    alias = 'market',
    materialized = 'view'
  )
}}

{{
  lending_aave_compatible_market_view(
    blockchain = 'fantom'
  )
}}
