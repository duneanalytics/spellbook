{{
  config(
    schema = 'aave_ethereum',
    alias = 'market_hourly_agg',
    materialized = 'view'
  )
}}

/*
{{
  lending_aave_compatible_market_hourly_agg_view(
    blockchain = 'ethereum'
  )
}}
*/

select 1 as dummy_placeholder 