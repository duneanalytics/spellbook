{{
  config(
    schema = 'aave_zksync',
    alias = 'interest_rates',
    materialized = 'view'
  )
}}

/*
{{
  lending_aave_compatible_interest_rates_view(
    blockchain = 'zksync'
  )
}}
*/

select 1 as dummy_placeholder 