{{
  config(
    schema = 'aave_avalanche_c',
    alias = 'supply',
    materialized = 'view'
    , post_hook='{{ hide_spells() }}'
  )
}}

{{
  lending_aave_compatible_supply_view(
    blockchain = 'avalanche_c'
  )
}} 