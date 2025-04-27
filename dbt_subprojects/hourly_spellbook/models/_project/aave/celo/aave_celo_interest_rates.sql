{{
  config(
    schema = 'aave_celo',
    alias = 'interest_rates',
    materialized = 'view'
  )
}}

/*
{{
  lending_aave_compatible_interest_rates_view(
    blockchain = 'celo'
  )
}}
*/

select 1 as dummy_placeholder 