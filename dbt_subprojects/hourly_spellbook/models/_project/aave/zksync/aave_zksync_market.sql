{{
  config(
    schema = 'aave_zksync',
    alias = 'market',
    materialized = 'view'
  )
}}

{{
  lending_aave_compatible_market_view(
    blockchain = 'zksync'
  )
}}
