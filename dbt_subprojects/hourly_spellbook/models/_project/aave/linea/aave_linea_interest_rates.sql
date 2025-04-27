{{
  config(
    schema = 'aave_linea',
    alias = 'interest_rates',
    materialized = 'view'
  )
}}

{{
  lending_aave_compatible_interest_rates_view(
    blockchain = 'linea'
  )
}}
