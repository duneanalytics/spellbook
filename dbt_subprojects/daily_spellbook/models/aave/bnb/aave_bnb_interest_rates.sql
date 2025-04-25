{{
  config(
    schema = 'aave_bnb',
    alias = 'interest_rates',
    materialized = 'view'
  )
}}

/*
{{
  lending_aave_compatible_interest_rates_view(
    blockchain = 'bnb'
  )
}}
*/

select 1 as dummy_placeholder 