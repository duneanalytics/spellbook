{{
  config(
    schema = 'aave_ethereum',
    alias = 'interest_rates',
    materialized = 'view'
  )
}}

{{
  lending_aave_compatible_interest_rates_view(
    blockchain = 'ethereum'
  )
}}
