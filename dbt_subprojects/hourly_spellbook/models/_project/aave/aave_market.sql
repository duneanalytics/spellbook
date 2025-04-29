{{
  config(
    schema = 'aave',
    alias = 'market',
    materialized = 'view'
  )
}}

{{
  lending_aave_compatible_market_view()
}}
