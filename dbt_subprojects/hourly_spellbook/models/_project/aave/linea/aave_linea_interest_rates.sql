{{
  config(
    schema = 'aave_linea',
    alias = 'interest_rates',
    materialized = 'view'
    , post_hook='{{ hide_spells() }}'
  )
}}

{{
  lending_aave_compatible_interest_rates_view(
    blockchain = 'linea'
  )
}}
