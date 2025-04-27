{{
  config(
    schema = 'aave_scroll',
    alias = 'interest_rates',
    materialized = 'view'
  )
}}

{{
  lending_aave_compatible_interest_rates_view(
    blockchain = 'scroll'
  )
}}
