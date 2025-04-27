{{
  config(
    schema = 'aave',
    alias = 'interest_rates',
    materialized = 'view'
  )
}}

/*
{{
  lending_aave_compatible_interest_rates_view()
}}
*/

select 1 as dummy_placeholder
