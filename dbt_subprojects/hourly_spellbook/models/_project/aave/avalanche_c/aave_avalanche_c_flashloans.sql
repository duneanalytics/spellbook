{{
  config(
    schema = 'aave_avalanche_c',
    alias = 'flashloans',
    materialized = 'view'
    , post_hook='{{ hide_spells() }}'
  )
}}

{{
  lending_aave_compatible_flashloans_view(
    blockchain = 'avalanche_c'
  )
}} 