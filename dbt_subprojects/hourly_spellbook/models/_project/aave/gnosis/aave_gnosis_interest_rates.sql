{{
  config(
    schema = 'aave_gnosis',
    alias = 'interest_rates',
    materialized = 'view'
    , post_hook='{{ hide_spells() }}'
  )
}}

{{
  lending_aave_compatible_interest_rates_view(
    blockchain = 'gnosis'
  )
}}
