{{
  config(
    schema = 'aave_scroll',
    alias = 'borrow',
    materialized = 'view'
    , post_hook='{{ hide_spells() }}'
  )
}}

{{
  lending_aave_compatible_borrow_view(
    blockchain = 'scroll'
  )
}} 