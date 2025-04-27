{{
  config(
    schema = 'aave_optimism',
    alias = 'interest_rates',
    materialized = 'view'
  )
}}

{{
  lending_aave_compatible_interest_rates_view(
    blockchain = 'optimism'
  )
}}
