{{
  config(
    schema = 'aave_gnosis',
    alias = 'interest_rates',
    materialized = 'view'
  )
}}

/*
{{
  lending_aave_compatible_interest_rates_view(
    blockchain = 'gnosis'
  )
}}
*/

select 1 as dummy_placeholder 