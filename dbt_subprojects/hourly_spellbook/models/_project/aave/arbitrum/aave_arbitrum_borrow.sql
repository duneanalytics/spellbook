{{
  config(
    schema = 'aave_arbitrum',
    alias = 'borrow',
    materialized = 'view'
    , post_hook='{{ hide_spells() }}'
  )
}}

{{
  lending_aave_compatible_borrow_view(
    blockchain = 'arbitrum'
  )
}}
