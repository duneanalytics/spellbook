{{
  config(
    schema = 'aave_avalanche_c',
    alias = 'market',
    materialized = 'view'
  )
}}

/*
{{
  lending_aave_compatible_market_view(
    blockchain = 'avalanche_c'
  )
}}
*/

select 1 as dummy_placeholder 