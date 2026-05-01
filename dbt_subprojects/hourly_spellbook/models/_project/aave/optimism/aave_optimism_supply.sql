{{
  config(
    schema = 'aave_optimism',
    alias = 'supply',
    materialized = 'view'
    , post_hook='{{ hide_spells() }}'
  )
}}

{{
  lending_aave_compatible_supply_view(
    blockchain = 'optimism'
  )
}} 