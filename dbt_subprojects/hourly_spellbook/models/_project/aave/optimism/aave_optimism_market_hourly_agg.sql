{{
  config(
    schema = 'aave_optimism',
    alias = 'market_hourly_agg',
    materialized = 'view'
  )
}}

/*
{{
  lending_aave_compatible_market_hourly_agg_view(
    blockchain = 'optimism'
  )
}}
*/

select 1 as dummy_placeholder 