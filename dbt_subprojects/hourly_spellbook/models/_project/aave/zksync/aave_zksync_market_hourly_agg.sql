{{
  config(
    schema = 'aave_zksync',
    alias = 'market_hourly_agg',
    materialized = 'view'
  )
}}

/*
{{
  lending_aave_compatible_market_hourly_agg_view(
    blockchain = 'zksync'
  )
}}
*/

select 1 as dummy_placeholder 