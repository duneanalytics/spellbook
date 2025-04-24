{{
  config(
    schema = 'aave_arbitrum',
    alias = 'market',
    materialized = 'view'
  )
}}

/*
{{
  lending_aave_compatible_market_view(
    blockchain = 'arbitrum'
  )
}}
*/

select 1 as dummy_placeholder
