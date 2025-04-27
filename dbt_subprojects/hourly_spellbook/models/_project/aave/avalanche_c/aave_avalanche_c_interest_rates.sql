{{
  config(
    schema = 'aave_avalanche_c',
    alias = 'interest_rates',
    materialized = 'view'
  )
}}

/*
{{
  lending_aave_compatible_interest_rates_view(
    blockchain = 'avalanche_c'
  )
}}
*/

select 1 as dummy_placeholder 