{{
  config(
    schema = 'aave_sonic',
    alias = 'interest_rates',
    materialized = 'view'
  )
}}

/*
{{
  lending_aave_compatible_interest_rates_view(
    blockchain = 'sonic'
  )
}}
*/

select 1 as dummy_placeholder 