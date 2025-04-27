{{
  config(
    schema = 'aave_fantom',
    alias = 'interest_rates',
    materialized = 'view'
  )
}}

/*
{{
  lending_aave_compatible_interest_rates_view(
    blockchain = 'fantom'
  )
}}
*/

select 1 as dummy_placeholder 