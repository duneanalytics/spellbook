{{
  config(
    schema = 'aave_optimism',
    alias = 'borrow',
    materialized = 'view'
    , post_hook='{{ hide_spells() }}'
  )
}}

{{
  lending_aave_compatible_borrow_view(
    blockchain = 'optimism'
  )
}} 