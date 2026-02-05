{{
  config(
    schema = 'aave_fantom',
    alias = 'supply',
    materialized = 'view'
    , post_hook='{{ hide_spells() }}'
  )
}}

{{
  lending_aave_compatible_supply_view(
    blockchain = 'fantom'
  )
}} 