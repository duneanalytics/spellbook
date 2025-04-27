{{
  config(
    schema = 'aave_zksync',
    alias = 'market',
    materialized = 'view'
  )
}}

/*
{{
  lending_aave_compatible_market_view(
    blockchain = 'zksync'
  )
}}
*/

select 1 as dummy_placeholder 