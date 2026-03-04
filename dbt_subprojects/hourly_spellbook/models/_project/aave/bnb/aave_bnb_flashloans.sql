{{
  config(
    schema = 'aave_bnb',
    alias = 'flashloans',
    materialized = 'view'
    , post_hook='{{ hide_spells() }}'
  )
}}

{{
  lending_aave_compatible_flashloans_view(
    blockchain = 'bnb'
  )
}} 