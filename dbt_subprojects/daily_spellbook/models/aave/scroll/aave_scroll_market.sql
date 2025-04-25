{{
  config(
    schema = 'aave_scroll',
    alias = 'market',
    materialized = 'view'
  )
}}

/*
{{
  lending_aave_compatible_market_view(
    blockchain = 'scroll'
  )
}}
*/

select 1 as dummy_placeholder 